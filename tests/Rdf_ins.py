
import unittest

import sys
import os

import json
import csv

sys.path.insert(0,(os.path.dirname(os.path.dirname(__file__))))

from dfitools.RedisDf import RedisDf

class TestRdf(unittest.TestCase):

    def setUp(self):
        self.rdf = RedisDf(key = 'testdata')
        self.rdf.delete('testdata')

        with open('tests/data/mtcars.csv') as f:
            rd = csv.DictReader(f)
            self.dat = [*rd]
        for l in self.dat:
            rep = json.dumps(l)
            self.rdf.rpush('testdata',rep)

    def tearDown(self):
        self.rdf.delete('testdata')

    # Tests: #######################

    def test_get(self):
        """
        Test getting data
        """
        res = self.rdf.getChunk()
        self.assertEqual(len(res),self.rdf.chunksize)


    def test_change(self):
        """
        Test changing the data, working chunk by chunk
        adding a new column.
        """
        prelen = self.rdf.llen(self.rdf.key)

        ch = self.rdf.getChunk()
        while ch is not None:

            for e in ch:
                e['hp2'] = int(e['hp']) * 2 

            self.rdf.writeChunk(ch)
            ch = self.rdf.getChunk()
    
        self.rdf.commit()

        #testrow = self.rdf.lrange('testdata',0,0)[0]
        #testrow = json.loads(testrow.decode())

        self.assertEqual(self.rdf.llen(self.rdf.key),prelen)
        #self.assertEqual(int(testrow['hp']) * 2,testrow['hp2'])

    def test_ncolNrow(self):
        """
        Test ncol, nrow methods
        """
        self.assertEqual(self.rdf.ncol(),len(self.dat[0].keys()))
        self.assertEqual(self.rdf.nrow(),len(self.dat))

    def test_validate(self):
        """
        Test validation, checks each row for ncol, must match with 1st. row
        Rdf validates @ commit if appending
        """

        self.rdf.validate()

        goodrow = dict(zip(self.rdf.names(),[*range(1,13)]))
        self.rdf.writeChunk([goodrow])
        self.rdf.commit()
        self.rdf.validate()
        self.assertEqual(self.rdf.nrow(),33)

        badrow = dict(zip([*range(1,13)],[*range(1,13)]))
        self.rdf.writeChunk([badrow])
        with self.assertRaises(AssertionError):
            self.rdf.commit()
            self.rdf.validate()

    #def test_failsWithOldData(self):

if __name__ == '__main__':
    unittest.main()
