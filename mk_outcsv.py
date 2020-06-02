import math
import sys
import sqlite3
import json
import functools
import collections

import requests

guids = collections.defaultdict(list)
name = collections.defaultdict(str)

con = sqlite3.connect('../db/arboreta.sqlite')
samples = [[row[0],row[1]] for row in con.execute('select name,guid from sample_lookup_table').fetchall()]

for sample_name, guid in samples:
    guids[sample_name].append(guid)
    name[guid] = sample_name
    
def get_guids(sample_name):
    return guids[sample_name]

def get_sample_name(guid):
    return name[guid]

s = requests.Session()

@functools.lru_cache(maxsize=None)
def get_neighbours(guid, reference, distance, quality, elephantwalkurl):
    url = "{0}/sample/findneighbour/snp/{1}/{2}/{3}/elephantwalk/{4}".format(elephantwalkurl, guid, reference, distance, quality)
    sys.stderr.write("Url: {0}\n".format(url))
    sys.stderr.flush()
    ret = s.get(url).json()
#    ret = json.loads(r.text)
    if not ret or ret[0] == "Err" or ret[0] == "Bad":
#        print("guid: {0}, elephantwalk returned: {1}".format(guid, ret))
        return []
    else:
#        print("guid: {0}, elephantwalk returned {1} neighbours".format(guid, len(ret)))
        return ret

def main():
    i = 0
    out_table = []

    with open(sys.argv[1]) as map_file:
        map_data = [x.split(',') for x in map_file.read().split('\n')]
        # print(map_data)
        sample_names = [ datum[2] for datum in map_data if len(datum) > 4 ]
        x = { datum[2]:int(datum[4]) for datum in map_data if len(datum) > 4 }
        y = { datum[2]:int(datum[5]) for datum in map_data if len(datum) > 4 }

        for sampleA_name in sample_names:
            sys.stderr.write("sample number: {0}\n".format(i))
            sys.stderr.flush()
            i = i + 1
            # print(sampleA_name)
            guidsA = get_guids(sampleA_name)
            sys.stderr.write('found {0} guids for sample name {1}\n'.format(len(guidsA), sampleA_name))
            sys.stderr.flush()
            # print(guidsA)
            for guid in guidsA:
                sampleB_disttable = []
                # print(guid)
                sampleB_disttable.append([guid, 0])
                neighbours = get_neighbours(guid, 'R00000039', '50', '0.80', 'http://192.168.7.90:9184')
                sys.stderr.write('found {0} neighbours for sample name {1} for guid {2}\n'.format(len(neighbours), sampleA_name, guid))
                sys.stderr.flush()
                if neighbours:
                    for neighbour in neighbours:
                        sampleB_disttable.append(neighbour)
                # print(sampleB_disttable)
                for sampleB_pair in sampleB_disttable:
                    sampleB_name = get_sample_name(sampleB_pair[0])
                    # print(sampleB_name)

                    if not sampleB_name in x:
                        # print("rejecting", sampleB_name)
                        continue

                    d_xy = str(math.sqrt((x[sampleA_name]-x[sampleB_name])*(x[sampleA_name]-x[sampleB_name])+
                                         (y[sampleA_name]-y[sampleB_name])*(y[sampleA_name]-y[sampleB_name])) / 1000)


                    print(",".join([guid,
                                    sampleA_name,
                                    sampleB_pair[0],
                                    sampleB_name,
                                    str(sampleB_pair[1]),
                                    str(x[sampleA_name]),
                                    str(y[sampleA_name]),
                                    str(x[sampleB_name]),
                                    str(y[sampleB_name]),
                                    d_xy]))

if __name__ == '__main__':
    main()
