import os
import uuid
import json
import sqlite3
import subprocess
import threading
import time
import yaml

import lib

from flask import Flask, request, render_template

con = sqlite3.connect('arboreta.sqlite', check_same_thread=False)
db_lock = threading.Lock()

from config import cfg

def demon_interface():
    #
    # read tree file
    #
    def _get_tree(guid, reference, distance, quality):
        with db_lock, con:
            elem = con.execute('select * from queue where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                               (guid,reference,distance,quality)).fetchall()
        if not elem:
            print("invariant failed: _get_neighbours")
            exit(1)
        tree = open("data/{0}/merged_fasta.treefile".format(elem[0][1])).read()
        return tree

    #
    # get neighbours, make multifasta file and run iqtree
    #
    def go(guid, run_uuid, reference, distance, quality, elephantwalkurl, cores, iqtreepath):
        neighbour_guids = lib.get_neighbours(guid, reference, distance, quality, elephantwalkurl)

        old_dir = os.getcwd()
        run_dir = "data/" + run_uuid

        os.makedirs(run_dir, exist_ok=False)
        os.chdir(run_dir)

        data = { "guid": guid, "run_guid": run_uuid, "reference": reference,
                 "distance": distance, "quality": quality, "elephantwalkurl": elephantwalkurl,
                 "cores": cores, "iqtreepath": iqtreepath }

        with open("settings.json", "w") as f:
            f.write(json.dumps(data))

        lib.concat_fasta(neighbour_guids, reference, "merged_fasta")

        print("running iqtree")
        ret = os.system("{0} -s merged_fasta -nt {1}".format(iqtreepath, cores))
        os.chdir(old_dir)
        return (ret, neighbour_guids)

    #
    # read queue table and run go() when there's an entry
    #
    while True:
        with db_lock, con:
            elem = con.execute('select * from queue order by epoch_added desc limit 1').fetchall()

        if elem:
            elem = elem[0]
            print("starting {0}", elem)
            started = str(int(time.time()))
            with db_lock, con:
                con.execute('update queue set status = "RUNNING" where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                            (elem[0], elem[4], elem[5], elem[6]))

            ret, neighbour_guids = go(elem[0], elem[1], elem[4], elem[5], elem[6], elem[3], cfg['iqtreecores'],
                                      "../../contrib/iqtree-1.6.5-Linux/bin/iqtree")
            ended = str(int(time.time()))
            tree = _get_tree(elem[0], elem[4], elem[5], elem[6])
            with db_lock, con:
                con.execute('delete from queue where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                            (elem[0], elem[4], elem[5], elem[6]))
                con.execute('insert into complete values (?,?,?,?,?,?,?,?,?,?,?)',
                            (elem[0], elem[1], elem[3], elem[4], elem[5], elem[6], elem[7], started, ended, json.dumps(neighbour_guids), tree))
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
    if not reference: reference = cfg['default_reference']
    distance = request.args.get('distance')
    if not distance: distance = cfg['default_distance']
    quality = request.args.get('quality')
    if not quality: quality = cfg['default_quality']

    with db_lock, con:
        queued = con.execute('select * from queue where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                             (guid,reference,distance,quality)).fetchall()
        completed = con.execute('select * from complete where sample_guid = ? and reference = ? and distance = ? and quality = ?',
                                (guid,reference,distance,quality)).fetchall()

    if queued and completed:
        print("invariant failed: queued and completed")
        exit(1)

    if completed:
        return completed[0][n]
    elif queued:
        return "run is already queued\n"
    else:
        run_uuid = str(uuid.uuid4())
        with db_lock, con:
            con.execute('insert into queue values (?,?,?,?,?,?,?,?)',
                        (guid, run_uuid, "queued", cfg['elephantwalkurl'], reference, distance, quality, str(int(time.time()))))
        return "run added to queue\n"


#
# flask routes
#
@app.route('/status')
def status():
    with db_lock, con:
        running = con.execute('select sample_guid, reference, distance, quality from queue where status = "RUNNING"').fetchall()
        queued = con.execute('select sample_guid, reference, distance, quality from queue where status <> "RUNNING"').fetchall()
        completed_ = con.execute('select sample_guid, reference, distance, quality, epoch_start, epoch_end from complete order by epoch_end desc').fetchall()
    completed = []
    daemon_alive = T.is_alive()
    for run in completed_:
        completed.append(list(run))
    for run in completed:
        run.append(lib.hms_timediff(run[5], run[4]))
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
    with db_lock, con:
        queued = con.execute('select sample_guid, reference, distance, quality from queue').fetchall()
    return json.dumps(queued)

@app.route('/complete')
def get_complete():
    with db_lock, con:
        completed = con.execute('select sample_guid, reference, distance, quality from complete').fetchall()
    return json.dumps(completed)
