"""
An interface to redis, which allows for chunked processing of data
located in a redis list. The data is separated by nl (\n), which means
that no \n can be present in cells or headers.
"""

import sys
from io import StringIO
import csv


class RedisCsvChannel():
    def __init__(self,redisCon,key,chunksize,verbose = False):
        self.verbose = verbose
        assert len(redisCon.keys(key)) > 0, 'No such key in redis %s'%(key)

        self.redis = redisCon
        self.chunksize = chunksize 
        self.key = key
        self.tmpkey = self.key + '_tmp'

        self.header = self.redis.lpop(self.key).decode()
        self.header = [*csv.reader([self.header])][0]

        self.redis.delete(self.tmpkey)

    def ncol(self):
        """
        Retrieve the number of columns (length of header)
        """
        return(len(self.header))

    def nrow(self):
        """
        Retrieve the number of rows stored in key
        """
        return(self.redis.llen(self.key))

    def appendCol(self,name):
        """
        Add a name to header, allowing you to append a value to each row.
        """
        self.header.append(name)

    def colIndex(self,name):
        """
        Retrieve the index (in each row) of a named column.
        """
        if name in self.header:
            return(self.header.index(name))
        else:
            raise AssertionError('name %s not a column'%(name))

    def getChunk(self):
        """
        Retrieve a chunk of data (a list of lists), popping it from the list
        """
        csvLines = []

        for i in range(self.chunksize):
            result = self.redis.lpop(self.key)

            tries = 0
            if result:
                if tries > 0 and self.verbose:
                    print('skipped %i lines before result'%(tries))
                
                csvLines.append(result.decode())
            elif tries < 5:
                tries += 1
            else:
                break
        rd = csv.reader(csvLines)
        lolformat = list([*rd])

        return(lolformat)

    def writeChunk(self,data): 
        """
        Write a chunk of data (a list of lists) to the list
        """

        strio = StringIO()
        wr = csv.writer(strio)
        wr.writerows(data)

        for row in data:
            rowCols = len(row)
            if rowCols != self.ncol():
                raise AssertionError('Wrong no. of columns: %i - %i'%(rowCols,self.ncol()))

        lines = strio.getvalue().splitlines()
        for l in lines:
            self.redis.rpush(self.tmpkey,l)
        return(len(lines))
    
    def commit(self,label = False):
        """
        Commit new chunks, stored in key_tmp.
        Either appends to key, or moves entire key_tmp to key.
        """

        if self.redis.llen(self.tmpkey) > 0:

            if self.redis.llen(self.key) == 0:
                self.redis.rename(self.tmpkey,self.key)

            else:
                while True:
                    e = self.redis.rpop(self.tmpkey)
                    if e:
                        self.redis.lpush(self.key,e)
                    else:
                        break
            if label:
                self.redis.lpush(self.key,self.toStrRep([self.header]))
                print('labelling')

            return(True)

        else:
            print('nothing to commit!')
            return(False)

    def dump(self,type = 'strrep'):
        """
        Dump (copy) the entire list as either type='strrep' or type='lol' (list of lists)
        Nondestructive.
        """
        rows = self.redis.lrange(self.key,0,-1)
        rows = [s.decode() for s in rows]
        rows = [','.join(self.header)] + rows
        res = '\n'.join(rows)

        if type == 'lol':
            res = self.toLol(res.splitlines())
            res = [r for r in res if r != []]

        return(res)

    def toLol(self,lines):
        """
        Internal method, to get lol-representation of csv-string
        """
        rd = csv.reader(lines)
        return(list([*rd]))

    def toStrRep(self,lol):
        """
        Internal method, to get string-representation of list-of-lists 
        """
        strio = StringIO()
        wr = csv.writer(strio)
        wr.writerows(lol)
        return(strio.getvalue())

