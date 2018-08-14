import json
import requests
import glob
import gzip

from config import cfg

import datetime
import dateutil.relativedelta
from collections import Counter

def hms_timediff(epochtime_start, epochtime_end):
    td = dateutil.relativedelta.relativedelta(datetime.datetime.fromtimestamp(float(epochtime_start)),
                                              datetime.datetime.fromtimestamp(float(epochtime_end)))
    return "{0}h {1}m {2}s".format(td.days * 24 + td.hours, td.minutes, td.seconds)

#
# fetch sample neighbours from elephantwalk
#
def get_neighbours(guid, reference, distance, quality, elephantwalkurl):
    url = "{0}/sample/findneighbour/snp/{1}/{2}/{3}/elephantwalk/{4}".format(elephantwalkurl, guid, reference, distance, quality)
    print("Url: {0}".format(url))
    r = requests.get(url)
    ret = json.loads(r.text)
    if ret[0] == "Err" or ret[0] == "Bad":
        print("guid: {0}, elephantwalk returned: {1}".format(guid, ret))
        return []
    else:
        print("guid: {0}, elephantwalk returned {1} neighbours".format(guid, len(ret)))
        return ret

#
# merge fasta files from guids into a multifasta file
#
def concat_fasta(guids, names, reference, out_file):
    with open(out_file, "w") as out:
        for n in range(len(guids)):
            guid = guids[n]
            name = names[n]
            print(guid)
            pattern = cfg['pattern'].format(guid, reference)
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

            out.write(">{0}_{1}\n".format(name, guid))
            out.write("{0}\n".format(fasta))

#
# count bases of output file from openmpsequencer, return counter of 'A','C','G','T'
# read the first line of the file: 
# model:A,C,G,A,T....
#
def count_bases(openmpsequencer_output):
   counter = Counter()

   with open(openmpsequencer_output) as f:
       lines = f.readlines()

   for line in lines:
       content = line.split(":")
       if "model" in content[0]:
           model = content[1].split(",")

   for entry in model:
       counter[entry] += 1

   return counter.most_common(4)
