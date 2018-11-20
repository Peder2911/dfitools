import redis
import json

class RedisDf(redis.Redis):
    """
    Use redis for multiprocess treatment of data
    "Data frame" in list-of-dictionaries format, stored in list
    Get chunk, write chunk and metadata (nrow / ncol / colnames)
    """
    def __init__(self,chunksize = 5,key = 'data',*args,**kwargs):

        super().__init__(*args,**kwargs)

        self.pl = self.pipeline()

        self.chunksize = chunksize
        self.key = key
        self.tmpkey = self.key + '_tmp' 

        if self.llen(self.key) != 0:
            self.ncols = self.ncol()
            self.checkCols = False
            self.validate()
        else:
            self.checkCols = True


    def getChunk(self):
        """
        Get a chunk of data (list of dictionaries) of self.chunksize length
        """

        for i in range(self.chunksize):
            self.pl.lpop(self.key)

        res = self.pl.execute()
        
        if any(res):
            res = [json.loads(i.decode()) for i in res if i is not None]
        else:
            res = None

        return(res)
        
    def writeChunk(self,chunk):
        """
        Write data to self.key_+'_tmp'
        """

        if self.checkCols:
            self.ncols = len(chunk[0].keys())
            self.checkCols = False
        else:
            ncols = self.ncols


        for row in chunk:
            rep = json.dumps(row)
            self.pl.rpush(self.tmpkey,rep)

        self.pl.execute()

    def commit(self):
        """
        Commit data in self.key + '_tmp' to self.key,
        checking ncol if appending (if data still in self.key)
        """

        if self.llen(self.key) == 0:
            self.rename(self.tmpkey,self.key)
        else:
            entry = self.lpop(self.tmpkey)

            while entry is not None:
                obj = json.loads(entry.decode())
                assert set(obj.keys()) == set(self.names()),"Bad row names %s"%(str(obj))
                self.rpush(self.key,entry)
                entry = self.lpop(self.tmpkey)

    def names(self):
        entry = self.lrange(self.key,0,0)[0].decode()
        entry = json.loads(entry)
        return(list(entry.keys()))
        
    def ncol(self):
        return(len(self.names()))

    def nrow(self,chunk = None):
        return(self.llen(self.key))

    def validate(self):
        """
        Check if all rows have equal number of columns
        """
        i = 0
        ncols = self.ncol()
        for row in self.lrange(self.key,0,-1):
            i = i + 1
            row = json.loads(row.decode())
            assert set(row.keys()) == set(self.names()), "Bad row at %i: %s"%(i,str(row))



    

    

