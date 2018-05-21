import os
import uuid
import json
import requests
import argparse
import glob
import gzip
import sqlite3
import subprocess
import threading
import time
import datetime
import dateutil.relativedelta
import yaml

from flask import Flask, request, render_template

con = sqlite3.connect('getree.sqlite', check_same_thread=False)
db_lock = threading.Lock()

with open("getree.yaml", "r") as f:
    config = yaml.load(f)
    _elephantwalkurl = config['elephantwalkurl']
    _pattern = config['pattern']

def demon_interface():
    #
    # fetch sample neighbours from elephantwalk
    #
    def get_neighbours(guid, reference, distance, quality, elephantwalkurl):
        url = "{0}/sample/findneighbour/snp/{1}/{2}/{3}/elephantwalk/{4}".format(elephantwalkurl, guid, reference, distance, quality)
        print("Url: {0}".format(url))
        r = requests.get(url)
        walkjson = json.loads(r.text)
        guids = []
        for entry in walkjson:
            guids.append(entry[0])
        if not guids:
            print("Error: elephantwalk returned 0 guids")
            exit(1)
        print("Found {0} guids".format(len(guids)))
        print(guids)
        return guids

    #
    # merge fasta files from guids into a multifasta file
    #
    def concat_fasta(guids, reference, out_file):
        with open(out_file, "w") as out:
            for guid in guids:
                print(guid)
                pattern = _pattern.format(guid, reference)
                files = glob.glob(pattern)
                if not files:
                    print("ERROR: Couldn't find file matching pattern {0}".format(pattern))
                    exit(1)
                if len(files) > 1:
                    print("ERROR: Found more than one file matching pattern {0}".format(pattern))
                    exit(1)
                print("processing {0}".format(files[0]))
                fasta_gzip = open(files[0], mode="rb").read()
                fasta = "".join(gzip.decompress(fasta_gzip).decode('ascii').split('\n')[1:])

                out.write(">{0}\n".format(guid))
                out.write("{0}\n".format(fasta))

    #
    # read tree file
    #
    def _get_tree(guid, reference, distance, quality):
        db_lock.acquire()
        elem = con.execute('select * from queue where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                           (guid,reference,distance,quality)).fetchall()
        con.commit()
        db_lock.release()
        if not elem:
            print("invariant failed: _get_neighbours")
            exit(1)
        tree = open("./{0}/merged_fasta.treefile".format(elem[0][1])).read()
        return tree

    #
    # get neighbours, make multifasta file and run iqtree
    #
    def go(guid, run_uuid, reference, distance, quality, elephantwalkurl, cores, iqtreepath):
        neighbour_guids = get_neighbours(guid, reference, distance, quality, elephantwalkurl)

        old_dir = os.getcwd()
        os.makedirs(run_uuid, exist_ok=False)
        os.chdir(run_uuid)

        data = { "guid": guid, "run_guid": run_uuid, "reference": reference,
                 "distance": distance, "quality": quality, "elephantwalkurl": elephantwalkurl,
                 "cores": cores, "iqtreepath": iqtreepath }

        with open("settings.json", "w") as f:
            f.write(json.dumps(data))

        concat_fasta(neighbour_guids, reference, "merged_fasta")

        print("running iqtree")
        ret = os.system("{0} -s merged_fasta -nt {1}".format(iqtreepath, cores))
        os.chdir(old_dir)
        return (ret, neighbour_guids)

    #
    # read queue table and run go() when there's an entry
    #
    while True:
        db_lock.acquire()
        elem = con.execute('select * from queue order by epoch_added desc limit 1').fetchall()
        con.commit()
        db_lock.release()

        if elem:
            elem = elem[0]
            print("starting {0}", elem)
            started = str(int(time.time()))
            db_lock.acquire()
            con.execute('update queue set status = "RUNNING" where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                        (elem[0], elem[4], elem[5], elem[6]))
            con.commit()
            db_lock.release()

            ret, neighbour_guids = go(elem[0], elem[1], elem[4], elem[5], elem[6], elem[3], 20,
                                      "../contrib/iqtree-1.6.5-Linux/bin/iqtree")
            ended = str(int(time.time()))
            tree = _get_tree(elem[0], elem[4], elem[5], elem[6])
            db_lock.acquire()
            con.execute('delete from queue where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                        (elem[0], elem[4], elem[5], elem[6]))
            con.execute('insert into complete values (?,?,?,?,?,?,?,?,?,?,?)',
                        (elem[0], elem[1], elem[3], elem[4], elem[5], elem[6], elem[7], started, ended, json.dumps(neighbour_guids), tree))
            con.commit()
            db_lock.release()
            print("done with {0}", elem)

        if int(time.time()) % 100 == 0: print("daemon idle")
        time.sleep(5)

T = threading.Thread(target=demon_interface)
T.start()

app = Flask(__name__)

#
# return nth column from run table. add run to queue if it doesn't exist
#
def get_run_index(guid, n):
    reference = request.args.get('reference')
    if not reference: reference = "R00000039"
    distance = request.args.get('distance')
    if not distance: distance = "20"
    quality = request.args.get('quality')
    if not quality: quality = "0.08"

    db_lock.acquire()
    queued = con.execute('select * from queue where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                         (guid,reference,distance,quality)).fetchall()
    completed = con.execute('select * from complete where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                            (guid,reference,distance,quality)).fetchall()
    con.commit()
    db_lock.release()

    if queued and completed:
        print("invariant failed: queued and completed")
        exit(1)

    if completed:
        return completed[0][n]
    elif queued:
        return "run is already queued\n"
    else:
        run_uuid = str(uuid.uuid4())
        db_lock.acquire()
        con.execute('insert into queue values (?,?,?,?,?,?,?,?)',
                    (guid,run_uuid,"queued",_elephantwalkurl,reference,distance,quality,str(int(time.time()))))
        con.commit()
        db_lock.release()
        return "run added to queue\n"

#
# flask routes
#
@app.route('/status')
def status():
    db_lock.acquire()
    running = con.execute('select sample_guid, reference, distance, quality from queue where status = "RUNNING"').fetchall()
    queued = con.execute('select sample_guid, reference, distance, quality from queue where status <> "RUNNING"').fetchall()
    completed_ = con.execute('select sample_guid, reference, distance, quality, epoch_start, epoch_end from complete').fetchall()
    con.commit()
    db_lock.release()
    completed = []
    daemon_alive = T.is_alive()
    for run in completed_:
        completed.append(list(run))
    for run in completed:
        td = dateutil.relativedelta.relativedelta(datetime.datetime.fromtimestamp(float(run[5])),
                                                  datetime.datetime.fromtimestamp(float(run[4])))
        run.append("{0}h {1}m {2}s".format(td.days * 24 + td.hours, td.minutes, td.seconds))
    return render_template('status.template', running=running, queued=queued, completed=completed, daemon_alive=daemon_alive)

@app.route('/neighbours/<guid>')
def get_neighbours(guid):
    return get_run_index(guid, 9)

@app.route('/tree/<guid>')
def get_tree(guid):
    return get_run_index(guid, 10)

@app.route('/new_run')
def new_run():
    guid = request.args.get('guid')
    return get_run_index(guid, 10)

@app.route('/queue')
def get_queue():
    db_lock.acquire()
    queued = con.execute('select sample_guid, reference, distance, quality from queue').fetchall()
    con.commit()
    db_lock.release()
    return json.dumps(queued)

@app.route('/complete')
def get_complete():
    db_lock.acquire()
    completed = con.execute('select sample_guid, reference, distance, quality from complete').fetchall()
    con.commit()
    db_lock.release()
    return json.dumps(completed)
