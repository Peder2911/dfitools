
from io import StringIO
import csv

"""
An interface to redis, which allows for chunked processing of data
located in a redis list. The data is separated by nl (\n), which means
that no \n can be present in cells or headers.
"""

class RedisCsvChannel():
    def __init__(self,redisCon,key,chunksize):
        assert len(redisCon.keys(key)) > 0, 'No such key in redis %s'%(key)

        self.redis = redisCon
        self.chunksize = chunksize 
        self.key = key
        self.tmpkey = self.key + '_tmp'

        self.header = self.redis.lpop(self.key).decode()
        self.header = [*csv.reader([self.header])][0]
        print(self.header)

        self.redis.delete(self.tmpkey)

    def getChunk(self):
        csvLines = []

        for i in range(self.chunksize):
            result = self.redis.lpop(self.key)
            if result:
                csvLines.append(result.decode())
            else:
                break

        rd = csv.reader(csvLines)
        lolformat = list([*rd])

        wrongCols = [len(lf) != len(self.header) for lf in lolformat] 
        if any(wrongCols):
            whichRow = wrongCols.index(True)
            badRow = lolformat[whichRow]
            brLen = len(badRow)
            badRow = str(badRow)
            raise csv.Error('Len %i - %s, expected %i cols'%(brLen,badRow,len(self.header)))
        else:
            pass

        return(lolformat)
    def writeChunk(self,data): 
        strio = StringIO()
        wr = csv.writer(strio)
        wr.writerows(data)

        lines = strio.getvalue().splitlines()
        for l in lines:
            self.redis.rpush(self.tmpkey,l)
        return(len(lines))

    def commit(self):
        headerrep = self.toStrRep([self.header])
        self.redis.lpush(self.tmpkey,headerrep)
        self.redis.rename(self.tmpkey,self.key)
        return(True)

    def toLol(self,lines):
        rd = csv.reader(lines)
        return(list([*rd]))

    def toStrRep(self,lol):
        strio = StringIO()
        wr = csv.writer(strio)
        wr.writerows(lol)
        return(strio.getvalue())

