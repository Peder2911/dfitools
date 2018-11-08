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

with open('tests/data/mtcars.csv') as fo:
    mtcars = fo.readlines()
nrows = len(mtcars)

for l in mtcars:
    r.rpush('testdata',l)

rcc = RedisCsvChannel.RedisCsvChannel(r,'testdata',5)
origHeader = rcc.header

# Run tests ########################

""" Read and double data """

ch = ['init']
while ch != []:
    ch = rcc.getChunk()
    print((colorama.Fore.BLUE + 'read...'), end = '')
    ch = ch + ch
    print((colorama.Fore.BLUE + 'doubled...'), end = '')
    rcc.writeChunk(ch)
    print((colorama.Fore.BLUE + 'written...'), end = '')

print('\n',end = '')

rcc.commit()

""" Test that it doubled the data """ 

try:
    assert r.llen('testdata') == (nrows * 2) - 1
except AssertionError:
    print((colorama.Fore.RED + 'Data not doubled: %i'%(nrows - r.llen)))
else:
    print((colorama.Fore.GREEN + 'Data is now twice as long!'))

print((colorama.Fore.GREEN + 'Test succeeded!'))
