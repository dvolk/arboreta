import json
import requests
import glob
import gzip
import os

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
#
#
def iterate_neighbours(guids, names, reference, pattern):
    for n in range(len(guids)):
        guid = guids[n]
        name = names[n]
        print(guid)
        pattern_final = pattern.format(guid, reference)
        print(pattern_final)
        files = glob.glob(pattern_final)
        if not files:
            print("ERROR: Couldn't find file matching pattern {0}".format(pattern_final))
            exit(1)
        if len(files) > 1:
            print("ERROR: Found more than one file matching pattern {0}".format(pattern_final))
            exit(1)
        print("processing {0}".format(files))
        yield(guid, name, files)

#
# merge fasta files from guids into a multifasta file
#
def concat_fasta(guids, names, reference, pattern, out_file):
    with open(str(out_file), "w") as out:
        for guid,name,files in iterate_neighbours(guids,names,reference,pattern):
            with open(files[0], mode="rb") as fasta_gzip_f:
                fasta_gzip = fasta_gzip_f.read()
                fasta = "".join(gzip.decompress(fasta_gzip).decode('ascii').split('\n')[1:])

                out.write(">{0}_{1}\n".format(name, guid))
                out.write("{0}\n".format(fasta))            

#
# create meta file for openmpsequencer, which has a list of guid and fasta file path
# line format: guid, space or tab, absolute path
#
def generate_openmpseq_metafile(guids, names, reference, pattern, out_file):
    with open(str(out_file), "w") as f:
        for guid,_,files in iterate_neighbours(guids,names,reference,pattern):
            f.write("{0}\t{1}\n".format(guid,files[0]))


#
#run openmpsequencer with metafile as input, produce output for count_bases
#
def run_openmpsequencer(openseq_bin_path, metafile, out_dir):
    os.system("{0} -t 1 -s {1} -o {2}".format(openseq_bin_path, metafile, out_dir))

#
# count bases of output file from openmpsequencer, return counter of 'A','C','G','T'
# read the first line of the file: 
# model:A,C,G,A,T....
#
def count_bases(openmpsequencer_output_filename):
   counter = Counter()

   with open(str(openmpsequencer_output_filename)) as f:
       lines = f.readlines()

   for line in lines:
       content = line.split(":")
       if "model" in content[0]:
           model = content[1].split(",")

   for entry in model:
       counter[entry] += 1

   return counter
