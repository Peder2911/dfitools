import unittest
import csv

import redis
from dfitools.RedisCsvChannel import RedisCsvChannel
from checkInR import checkInR
from subprocess import SubprocessError

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
        
    # ##############################

    def test_addrows(self):
        """
        Add some rows, check if exact number is added
        """

        initial = self.rcc.nrow()
        testrows = [[1,2,3,4,5,6,7,8,9,10,11,'something'] for i in range(10)]
        self.rcc.writeChunk(testrows)
        self.rcc.commit()
        
        post = self.rcc.nrow()
        self.assertEqual(initial + 10,post)

        self.assertEqual(checkInR(self.rcc.dump())['rows'],self.rcc.nrow())

    def test_catchBadRow(self):
        """
        Catch a row with too few columns
        """

        badTestRows= [[1,2,3,4,5,6,7,8,9,10,11,'something'] for i in range(10)]
        badTestRows[5] = [1,2,3,4,5,6,7,8,9,10,11,12,13]

        with self.assertRaises(AssertionError):
            self.rcc.writeChunk(badTestRows)

    def test_filterrows(self):
        """
        Filter out some rows based on a criterion
        """

        pre = self.rcc.nrow()
        tgt = self.rcc.header.index('hp')

        while True:
            ch = self.rcc.getChunk()
            if ch:
                ch = [row for row in ch if int(row[tgt]) > 140]
                self.rcc.writeChunk(ch)
            else:
                break

        self.rcc.commit()
        post = self.rcc.nrow()

        self.assertLess(post,pre)

        self.assertEqual(checkInR(self.rcc.dump())['rows'],self.rcc.nrow())

    def test_addCol(self):
        """
        Add a new column
        """
        preNcol = self.rcc.ncol()
        preNrow = self.rcc.nrow()

        tgt = self.rcc.colIndex('hp')
        self.rcc.appendCol('doubleHp')

        def transformRow(row,tgtindex):
            row.append(int(row[tgtindex]) + int(row[tgtindex]))
            return(row)
        
        while True:
            ch = self.rcc.getChunk()
            if ch:
                ch = [transformRow(row,tgt) for row in ch]
                self.rcc.writeChunk(ch)
            else:
                break

        self.rcc.commit()

        self.assertEqual(preNcol + 1,self.rcc.ncol())
        self.assertEqual(preNrow,self.rcc.nrow())

        self.assertEqual(checkInR(self.rcc.dump())['columns'],self.rcc.ncol())
        
if __name__ == '__main__':
    unittest.main()
