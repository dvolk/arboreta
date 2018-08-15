import unittest
import tempfile
import pathlib

import lib

class Test_Lib(unittest.TestCase):
    def setUp(self):
        self.guids = ["0c22021d-7ef9-4872-a9ad-0665ab485ad3",
                      "0f67261f-f1e6-4ba6-9ae9-43ab8e3d89d1",
                      "1a523b78-e1e7-4a58-a8d4-7c9c8de2170c"]
        self.names = ['a', 'b', 'c']
        self.reference = 'R00000039'
        self.pattern = "./tests/data/{0}_v3.fasta.gz"

    def test_iterate_neighbours(self):

        result = lib.iterate_neighbours(self.guids,self.names, self.reference, self.pattern)
        result = list(result)

        self.assertEqual(result[0], ('0c22021d-7ef9-4872-a9ad-0665ab485ad3', 'a', ['./tests/data/0c22021d-7ef9-4872-a9ad-0665ab485ad3_v3.fasta.gz']))
        self.assertEqual(result[1], ('0f67261f-f1e6-4ba6-9ae9-43ab8e3d89d1', 'b', ['./tests/data/0f67261f-f1e6-4ba6-9ae9-43ab8e3d89d1_v3.fasta.gz']))
        self.assertEqual(result[2], ('1a523b78-e1e7-4a58-a8d4-7c9c8de2170c', 'c', ['./tests/data/1a523b78-e1e7-4a58-a8d4-7c9c8de2170c_v3.fasta.gz']))

    def test_concat_fasta(self):
        out_file = tempfile.mktemp()
        lib.concat_fasta(self.guids,self.names,self.reference, self.pattern, out_file)
        self.assertTrue(pathlib.Path(out_file).is_file())
        with open(out_file) as f:
            result = f.readlines()
            self.assertEqual(len(result),6)
            for i, line in enumerate(result):
                n = int(i / 2)
                if i % 2 == 1:
                    self.assertTrue(len(line) == 4411533)
                else:
                    self.assertEqual(line.strip(), ">{0}_{1}".format(self.names[n], self.guids[n]))

    def test_generate_meta(self):
        out_file = tempfile.mktemp()
        lib.generate_openmpseq_metafile(self.guids,self.names,self.reference, self.pattern, out_file)
        self.assertTrue(pathlib.Path(out_file).is_file())
        print(out_file)
        with open(out_file) as f:
            result = f.readlines()
            self.assertEqual(len(result),3)
            self.assertTrue(result[0],"0c22021d-7ef9-4872-a9ad-0665ab485ad3\t./tests/data/0c22021d-7ef9-4872-a9ad-0665ab485ad3_v3.fasta.gz")
            self.assertTrue(result[1],"0f67261f-f1e6-4ba6-9ae9-43ab8e3d89d1\t./tests/data/0f67261f-f1e6-4ba6-9ae9-43ab8e3d89d1_v3.fasta.gz")
            self.assertTrue(result[2],"1a523b78-e1e7-4a58-a8d4-7c9c8de2170c\t./tests/data/1a523b78-e1e7-4a58-a8d4-7c9c8de2170c_v3.fasta.gz")

    def test_run_openmpsequencer(self):
        metafile = "./tests/data/meta.txt"
        out_dir = tempfile.mkdtemp()
        lib.run_openmpsequencer("./contrib/openmpSequence", metafile, out_dir)
        out_count_bases_file = pathlib.Path(out_dir) / "sequencer_count_bases.txt"
        print(out_count_bases_file)
        self.assertTrue(out_count_bases_file.is_file())
        counts = lib.count_bases(out_count_bases_file)
        print(counts)
        counts_acgt = [counts[base] for base in ['A', 'C', 'G', 'T']]
        self.assertEqual(counts_acgt, [694854, 1318851, 1314135, 695061])
        
