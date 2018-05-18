import os
import uuid
import json
import requests
import argparse
import pathlib
import glob
import gzip
import string

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
    return guids

def concat_fasta(guids, reference, out_file):
    out = open(out_file, "w")
    for guid in guids:
        pattern = "/mnt/microbio/ndm-hicf/ogre/pipeline_output/{0}/MAPPING/*_{1}/STD/basecalls/{0}*.fasta.gz".format(guid, reference)
        files = glob.glob(pattern)
        if not files:
            print("ERROR: Couldn't find file matching pattern {0}".format(pattern))
            exit(1)
        if len(files) > 1:
            print("ERROR: Found more than one file matching pattern {0}".format(pattern))
            exit(1)
        #print("processing {0}".format(files[0]))
        fasta_gzip = open(files[0], mode="rb").read()
        fasta = "".join(gzip.decompress(fasta_gzip).decode('ascii').split('\n')[1:])

        out.write(">{0}\n".format(guid))
        out.write("{0}\n".format(fasta))

def go():
    parser = argparse.ArgumentParser()
    parser.add_argument("guid", help="guid to build tree from")
    parser.add_argument("--reference", help="reference", default="R00000039")
    parser.add_argument("--distance", help="distance", default="20")
    parser.add_argument("--quality", help="quality",default="0.80")
    parser.add_argument("--elephantwalkurl", help="elephant walk url", default="http://192.168.7.90:9184")
    parser.add_argument("--cores", help="iqtree cores", default="20")
    parser.add_argument("--iqtreepath", help="relative path to iqtree", default="../contrib/iqtree-1.6.5-Linux/bin/iqtree")
    args = parser.parse_args()

    print("Sample guid: {0}".format(args.guid))
    print("Reference: {0}".format(args.reference))
    print("Distance: {0}".format(args.distance))
    print("Quality: {0}".format(args.quality))
    print("Elephant walk url: {0}".format(args.elephantwalkurl))

    neighbour_guids = get_neighbours(args.guid, args.reference, args.distance, args.quality, args.elephantwalkurl)

    run_uuid = str(uuid.uuid4())

    os.makedirs(run_uuid, exist_ok=False)
    os.chdir(run_uuid)

    data = { "guid": args.guid,
             "run_guid": run_uuid,
             "reference": args.reference,
             "distance": args.distance,
             "quality": args.quality,
             "elephantwalkurl": args.elephantwalkurl,
             "cores": args.cores,
             "iqtreepath": args.iqtreepath
             }

    with open("settings.json", "w") as f:
        f.write(json.dumps(data))

    concat_fasta(neighbour_guids, args.reference, "merged_fasta")

    print("running iqtree")
    os.system("{0} -s merged_fasta -nt {1}".format(args.iqtreepath, args.cores))

def main():
    go()

if __name__ == '__main__':
    main()
