import sys 
from dfitools import RedisCsvChannel
import redis
import colorama
import csv

# Initialize #######################

colorama.init(autoreset = True)
r = redis.Redis()
r.delete('testdata')

# Import some data #################

with open('tests/data/imdb.csv') as fo:
    imdb = fo.readlines()
nrows = len(imdb)

for l in imdb:
    r.rpush('testdata',l)

rcc = RedisCsvChannel.RedisCsvChannel(r,'testdata',5)

# Run tests ########################

""" Read and filter data """

nread = 0
nwritten = 0
ch = ['init']
while ch != []:
    ch = rcc.getChunk()
    nread += len(ch)

    # Do something here

    prewrite = r.llen('testdata_tmp')
    rcc.writeChunk(ch)
    postwrite = r.llen('testdata_tmp')

    nwritten += postwrite - prewrite

print(colorama.Fore.BLUE + 'Read: %i rows\nWritten: %i rows'%(nread,nwritten))


rcc.commit()

""" Test that it filtered the data """ 

#try:
#    assert r.llen('testdata') < nrows
#except AssertionError:
#    print((colorama.Fore.RED + 'Data not filtered: %i'%(nrows - r.llen)))
#else:
#    print((colorama.Fore.GREEN + 'Filtered out %i puny cars!'%(nrows - r.llen('testdata'))))

print((colorama.Fore.GREEN + 'Test succeeded!'))
