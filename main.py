import os
import sys
import tempfile
import uuid
import json
import sqlite3
import subprocess
import threading
import time
import yaml

import lib

from flask import Flask, request, render_template, make_response, redirect, abort

from matplotlib.backends.backend_svg import FigureCanvasSVG as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator

from io import StringIO

from config import cfg

con = sqlite3.connect(cfg['sqlitedbfilepath'], check_same_thread=False)
db_lock = threading.Lock()

class captured_output:
    def __init__(self):
        self.prevfd = None
        self.prev = None

    def __enter__(self):
        F = tempfile.NamedTemporaryFile()
        self.prevfd = os.dup(sys.stdout.fileno())
        os.dup2(F.fileno(), sys.stdout.fileno())
        self.prev = sys.stdout
        sys.stdout = os.fdopen(self.prevfd, "w")
        return F

    def __exit__(self, exc_type, exc_value, traceback):
        os.dup2(self.prevfd, self.prev.fileno())
        sys.stdout = self.prev

def graph(guids, reference, quality, elephantwalkurl):
    with db_lock, con:
        all_neighbours = con.execute("select distance,neighbours_count from neighbours where samples = ? and reference = ? and quality = ? and elephantwalkurl = ? order by distance asc",
                                     (guids, reference, quality, elephantwalkurl)).fetchall()
    return [(x[0], x[1]) for x in all_neighbours]

#
# same as graph, different format
#
def graph2(guids, reference, quality, elephantwalkurl):
    with db_lock, con:
        all_neighbours = con.execute("select distance,neighbours_count from neighbours where samples = ? and reference = ? and quality = ? and elephantwalkurl = ? order by distance asc",
                                     (guids, reference, quality, elephantwalkurl)).fetchall()
    return ([x[0] for x in all_neighbours], [x[1] for x in all_neighbours])

def graph3(guids, reference, quality, elephantwalkurl, cutoff):
    running = True
    ns = []
    last = None
    last_count = 0
    distance = 0
    ps = []
    while running:
        last = len(ns)
        ns = neighbours(guids, reference, distance, quality, elephantwalkurl)
        ps.append([distance, len(ns)])
        distance = distance + 1
        if len(ns) == last:
            last_count = last_count + 1
        else:
            last_count = 0
        if last_count >= cutoff - 1:
            running = False
        print(last_count)
    return ([x[0] for x in ps], [x[1] for x in ps])


#
# check if neighbours in database. query elephantwalk if not
#
# just returns guids argument if it is a list (contains a ,)
#
def neighbours(guids, reference, distance, quality, elephantwalkurl):
    with db_lock, con:
        neighbours = con.execute("select * from neighbours where samples = ? and reference = ? and distance = ? and quality = ? and elephantwalkurl = ?",
                                 (guids, reference, int(distance), quality, elephantwalkurl)).fetchall()
    if neighbours:
        print("returning from db")
        return json.loads(neighbours[0][7])
    else:
        if "," in guids:
            print("sample is a list of guids: returning itself")
            return [x.strip() for x in guids.split(",")]
        else:
            print("getting neighbours from elephantwalk")
            neighbour_guids = lib.get_neighbours(guids, reference, distance, quality, elephantwalkurl)
            with db_lock, con:
                uid = uuid.uuid4()
                n = con.execute("insert into neighbours values (?,?,?,?,?,?,?,?,?)",
                                (str(uid),guids,int(distance),reference,quality,elephantwalkurl,str(int(time.time())),json.dumps(neighbour_guids),len(neighbour_guids)))
            return neighbour_guids

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
        neighbour_guids = neighbours(guid, reference, distance, quality, elephantwalkurl)

        old_dir = os.getcwd()
        run_dir = "data/" + run_uuid

        print("makedirs")
        os.makedirs(run_dir, exist_ok=False)
        os.chdir(run_dir)

        data = { "guid": guid, "run_guid": run_uuid, "reference": reference,
                 "distance": distance, "quality": quality, "elephantwalkurl": elephantwalkurl,
                 "cores": cores, "iqtreepath": iqtreepath }

        with open("settings.json", "w") as f:
            f.write(json.dumps(data))

        if "," not in guid:
            neighbour_guids.append(guid)

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

            # with captured_output() as E:
            ret, neighbour_guids = go(elem[0], elem[1], elem[4], elem[5], elem[6], elem[3], cfg['iqtreecores'],
                                      "../../contrib/iqtree-1.6.5-Linux/bin/iqtree")
            # print(E.name)

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


@app.route('/')
def root_page():
    return redirect('/status')

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
    reference = request.args.get('reference')
    if not reference: reference = cfg['default_reference']
    distance = request.args.get('distance')
    if not distance: distance = cfg['default_distance']
    quality = request.args.get('quality')
    if not quality: quality = cfg['default_quality']
    return json.dumps(neighbours(guid, reference, distance, quality, cfg['elephantwalkurl']))

@app.route('/tree/<guid>')
def get_tree(guid):
    return get_run_index(guid, 10)

@app.route('/ndgraph/<guid>')
def get_graph(guid):
    reference = request.args.get('reference')
    if not reference: reference = cfg['default_reference']
    quality = request.args.get('quality')
    if not quality: quality = cfg['default_quality']
    return json.dumps(graph(guid, reference, quality, cfg['elephantwalkurl']))

@app.route('/ndgraph2/<guid>')
def get_graph2(guid):
    reference = request.args.get('reference')
    if not reference: reference = cfg['default_reference']
    quality = request.args.get('quality')
    if not quality: quality = cfg['default_quality']
    return json.dumps(graph2(guid, reference, quality, cfg['elephantwalkurl']))

@app.route('/ndgraph3/<guid>')
def get_graph3(guid):
    reference = request.args.get('reference')
    if not reference: reference = cfg['default_reference']
    quality = request.args.get('quality')
    if not quality: quality = cfg['default_quality']
    cutoff = request.args.get('cutoff')
    if not cutoff: cutoff = 4
    return json.dumps(graph3(guid, reference, quality, cfg['elephantwalkurl'], int(cutoff)))

@app.route('/ndgraph.svg/<guid>')
def get_graph_svg(guid):
    reference = request.args.get('reference')
    if not reference: reference = cfg['default_reference']
    quality = request.args.get('quality')
    if not quality: quality = cfg['default_quality']
    cutoff = request.args.get('cutoff')
    if cutoff:
        (xs,ys) = graph3(guid, reference, quality, cfg['elephantwalkurl'], int(cutoff))
    else:
        (xs,ys) = graph2(guid, reference, quality, cfg['elephantwalkurl'])
    slopes = [0]
    print(xs)
    for n in range(len(xs)):
        if n == 0: continue
        slopes.append((ys[n] - ys[n-1])/(xs[n]-xs[n-1]))
    fig = Figure(figsize=(12,7), dpi=100)
    fig.suptitle("Sample: {0}, reference: {1}, quality: {2}, ew: {3}".format(guid,reference,quality, cfg['elephantwalkurl']))
    ax = fig.add_subplot(111)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.plot(xs, ys, 'gx-', linewidth=1)
    ax.plot(xs, slopes, 'r-', linewidth=1)
    ax.set_xlabel("Distance")
    ax.set_ylabel("Neighbours")
    canvas = FigureCanvas(fig)
    svg_output = StringIO()
    canvas.print_svg(svg_output)
    response = make_response(svg_output.getvalue())
    response.headers['Content-Type'] = 'image/svg+xml'
    return response

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

@app.route('/lookup/<name>')
def lookup(name):
    try:
        guid = uuid.UUID(name)
        print("{0} looks like a uuid to me".format(name))
    except:
        guid = None
        print("{0} doesn't look like a uuid to me".format(name))

    with db_lock, con:
        if guid:
            rows = con.execute('select guid,name from sample_lookup_table where guid = ?', (name,)).fetchall()
        else:
            rows = con.execute("select guid,name from sample_lookup_table where upper(name) like ?", (name+"%",)).fetchall()
    print(rows)
    if rows:
        return json.dumps(rows)
    else:
        abort(404)

@app.route('/sync_sample_lookup_table')
def sync_lookup_table():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    cas_auth_provider = PlainTextAuthProvider(username=cfg['cassandra_username'], password=cfg['cassandra_password'])
    cas_cluster = Cluster(cfg['cassandra_ips'], auth_provider=cas_auth_provider)
    cas_session = cas_cluster.connect('nosql_schema')

    rows = cas_session.execute('select name,id from sample')
    with db_lock, con:
        con.execute('delete from sample_lookup_table')
        for row in rows:
            con.execute('insert into sample_lookup_table values (?, ?)', (str(row.id), row.name))

    cas_cluster.shutdown()
    return redirect('/')
