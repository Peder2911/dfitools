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

with open('tests/data/imdb_badheader.csv') as fo:
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

i = 0
while ch != []:
    i += 1
    try:
        ch = rcc.getChunk()
    except csv.Error as e:
        print(str(e))
        print(colorama.Fore.GREEN + 'Caught bad row @ %i'%(i))
        print((colorama.Fore.GREEN + 'Test succeeded!'))
        sys.exit(0)

sys.exit(1)


