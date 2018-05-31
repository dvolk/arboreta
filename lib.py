import json
import requests
import glob
import gzip

from config import cfg

import datetime
import dateutil.relativedelta

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
    walkjson = json.loads(r.text)
    guids = []
    for entry in walkjson:
        guids.append(entry[0])
    if not guids:
        print("Error: elephantwalk returned 0 guids")
        exit(1)
    print("Found {0} guids".format(len(guids)))
    #print(guids)
    return guids

#
# merge fasta files from guids into a multifasta file
#
def concat_fasta(guids, reference, out_file):
    with open(out_file, "w") as out:
        for guid in guids:
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

            out.write(">{0}\n".format(guid))
            out.write("{0}\n".format(fasta))

