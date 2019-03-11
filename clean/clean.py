#
# Some apha data came with sample names that don't match what we have
# in cassandra
# 
# The pattern is that apha samples names have too many leading zeroes
# in the sample sequence portion of the sample name
#
# To fix this we have to add sample names that are in cassandra and
# also try sample names with the leading zeroes removed
#
# so e.g. for sample name AF-12-00123-12 we would check if any of the
# samples
#
# AF-12-00123-12
# AF-12-0123-12
# AF-12-123-12
#
# are in cassandra, and add all the ones that are, duplicating original
# recording. Hidden in this is the assumption that these samples
# are all the same, i.e. that leading zeroes do not matter.
#

import requests
import sys
import sqlite3
import json
import functools

con = sqlite3.connect('/tmp/arboreta.sqlite')
sample_dict = { row[1]:row[0] for row in con.execute('select * from sample_lookup_table').fetchall() }

def does_it_exist(name, con):
    return name in sample_dict

def main():
    out_table = []

    with open(sys.argv[1], encoding='utf-8-sig') as map_file:
        map_data = [x.split(',') for x in map_file.read().split('\n')]
        map_data_ = []

        for datum in map_data:
            if not datum: continue
            #print(datum)
            name = datum[2]
            #print(name)

            tries = 0
            front = name[6]
            if does_it_exist(name, con):
                #print("found {0} after {1} tries".format(name, tries))
                map_data_.append([*datum[0:2], name, *datum[3:]])

            while front == '0':
                af, lab, seq, year = name.split('-')
                seq = seq[1:]
                name = "-".join([af, lab, seq, year])
                if not seq:
                    break
                front = seq[0]
                tries = tries + 1

                if does_it_exist(name, con):
                    #print("found {0} after {1} tries".format(name, tries))
                    map_data_.append([*datum[0:2], name, *datum[3:]])
                else:
                    #print("couldn't find guid for", datum[2], "after", tries, "tries. reduced to", name)
                    pass

        for datum_ in map_data_:
            print(",".join(datum_))

if __name__ == '__main__':
    main()