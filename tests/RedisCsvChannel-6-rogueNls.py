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

with open('tests/data/imdb_rogue_nls.csv') as fo:
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

try:
    assert nread == nwritten
except AssertionError:
    print(colorama.Fore.RED + 'Input and output differs!')
    print(colorama.Fore.YELLOW + 'input : %i\noutput : %i'%(nread,nwritten))
else:
    print((colorama.Fore.GREEN + 'Test succeeded!'))
    rcc.commit()


