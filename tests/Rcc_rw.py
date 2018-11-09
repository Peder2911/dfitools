import unittest
import csv

import redis
from dfitools.RedisCsvChannel import RedisCsvChannel
from checkInR import checkInR

class TestRcc(unittest.TestCase):
    """
    Tests reading from / working with a redis list containing mtcars data
    """

    def setUp(self):
        self.r = redis.Redis()
        self.r.delete('testdata')

        with open('tests/data/mtcars.csv') as f:
            self.testdata = f.readlines()
        for l in self.testdata:
            self.r.rpush('testdata',l)

        self.rcc = RedisCsvChannel(self.r,'testdata',5)

    def tearDown(self):
        self.r.delete('testdata')

    # Tests: #######################

    def test_writeOK(self):
        """
        Writes nrow -1 (header) to redis
        """
        self.assertEqual(self.r.llen('testdata'),len(self.testdata)-1)

    def test_headerOK(self):
        """
        Header is set to 1st. line in redislist
        """
        tdat = list(csv.reader(self.testdata))[0]
        self.assertEqual(self.rcc.header,tdat)

    def test_read(self):
        """
        Data is equally represented after writing in, and reading out
        """
        ch = ['init']

        chunks = [self.rcc.header]
        while ch != []:
            ch = self.rcc.getChunk()
            chunks = chunks + ch

        tdat = list(csv.reader(self.testdata))
        self.assertEqual(chunks,tdat)

    def test_rdump(self):
        """
        Data is OK after dump (checked by R)
        """
        resultsPre = checkInR('\n'.join(self.testdata))
        datastring = self.rcc.dump()
        resultsPost = checkInR(datastring)

        self.assertEqual(resultsPre,resultsPost)
    
    def test_loldump(self):
        """
        Test dump of list-of-lists
        """
        lol = self.rcc.dump(type = 'lol')
        ncol = len(lol[0])

        self.assertEqual(self.rcc.header,lol[0])
        self.assertEqual(len(self.testdata),len(lol))
        self.assertEqual(len(lol[0]), len(self.testdata[0].split(',')))
        """
        All columns are equal
        """
        for r in lol:
            self.assertEqual(len(r),ncol)
        

if __name__ == '__main__':
    unittest.main()
