# This is to run through all guids in cleaned sample files and try very guids in EW
# Input: ../db/arboreta.sqlite
# Input: ./data/Cattle_db_timestamp-CLEAN.csv
# Input: EW API
# Run: python3 elephantrun.py ../data/Cattle_db_20200601_120000-CLEAN.tsv > ../data/elephant_out.csv
# Output file:
# AF-21-07805-17,ec965a7f-a013-480c-888d-979146df3ccb,BAD sequence,-3                                                                                                                # AF-21-07805-17,add7c1b5-301c-4578-86dd-519fa2838400,OK,3                                                                                                                           
# AF-12-00180-18,d4ea27dd-b1d6-4b37-861a-a2b0f4adcdcf,OK,10                                                                                                                          
# AF-61-04461-19,a24dbb0a-abb9-4f3d-8110-59f392278ffe,Error Sample,-5 


import requests
import sqlite3
import collections
import sys

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

def get_neighbours(guid, reference='R00000039', distance='50', quality='0.80', elephantwalkurl='http://192.168.7.90:9184'):
    url = "{0}/sample/findneighbour/snp/{1}/{2}/{3}/elephantwalk/{4}".format(elephantwalkurl, guid, reference, distance, quality)
    ret1 = s.get(url).json()

    # elephant walk did not return a json list
    if type(ret1) != list:
        return "No result", -10, ret1
    # sample OK but has no neighbours
    if len(ret1) == 0:
        return "Empty result", 0, ret1
    # missing sample
    if ret1[0] == "Err":
        return "Error Sample", -5, ret1
    # bad sample
    if ret1[0] == "Bad":
        return "BAD sequence", -3, ret1
    # > 0 neighbours
    return "OK", len(ret1)

def go(sample_names):
    for i, sample_name in enumerate(sample_names):
        guids = get_guids(sample_name)
        for guid in guids:
            e_out = query_elephantwalk(guid)
            print("{0},{1},{2},{3},{4}".format(sample_name, guid, *e_out))
            sys.stderr.write("{0}. {1} {2} {3} {4}\n".format(i, sample_name, guid, *e_out))
            sys.stderr.flush()
    
if __name__ == "__main__":
    with open(sys.argv[1]) as f:
        xs = f.readlines()
    
    go([x.strip().split(',')[2] for x in xs])
