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
whereIsHp = rcc.header.index('hp')
meanHp = 147

# Run tests ########################

""" Read and filter data """

ch = ['init']
while ch != []:
    ch = rcc.getChunk()
    print((colorama.Fore.BLUE + '* read %i...'%(len(ch))), end = '')
    orig = len(ch)
    ch = [row for row in ch if int(row[whereIsHp]) > meanHp]
    print((colorama.Fore.BLUE + 'filtered out %i...'%(orig - len(ch))), end = '')
    rcc.writeChunk(ch)
    print((colorama.Fore.BLUE + 'written %i...'%(len(ch))), end = '')

print('\n',end = '')

rcc.commit()

""" Test that it filtered the data """ 

try:
    assert r.llen('testdata') < nrows
except AssertionError:
    print((colorama.Fore.RED + 'Data not filtered: %i'%(nrows - r.llen)))
else:
    print((colorama.Fore.GREEN + 'Filtered out %i puny cars!'%(nrows - r.llen('testdata'))))

print((colorama.Fore.GREEN + 'Test succeeded!'))
