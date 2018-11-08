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

""" Finds index of column """
try:
    whereIsName = rcc.header.index('name')
except Exception as e:
    print((colorama.Fore.RED + str(rcc.header)))
    print((colorama.Fore.RED + 'Something went wrong' + str(e)))
else:
    print((colorama.Fore.BLUE + 'Found index of named column'))

""" Extracts value of column for each row """
""" Writes chunks back to redis """
print((colorama.Fore.BLUE + 'Extracting row values'))

ch = ['init']

while ch != []:
    ch = rcc.getChunk()
    [print(rw[whereIsName]) for rw in ch]
    rcc.writeChunk(ch)

print((colorama.Fore.BLUE + 'Did something to all rows!'))

""" Commits the change """
tdlen = r.llen('testdata_tmp')
rcc.commit()
try:
    assert tdlen + 1 == r.llen('testdata')
except AssertionError:
    print((colorama.Fore.RED + 'commit changed length of data:'))
    print((colorama.Fore.BLUE + 'before: %i'%(tdlen)))
    print((colorama.Fore.RED + 'after: %i'%(r.llen('testdata'))))
else:
    print((colorama.Fore.BLUE + 'Commit ok'))

""" Nondestructive test - does not mess up data """
# Does not truncate data
try:
    assert r.llen('testdata') == nrows
except AssertionError:
    print((colorama.Fore.RED + 'Unequal number of rows: %i'%(nrows - r.llen)))
else:
    print((colorama.Fore.GREEN + 'Rows are equal before and after read / write'))

# Same header before and after 
header = r.lrange('testdata',0,0)
header = [bts.decode() for bts in header]
header = [*csv.reader(header)][0]

try:
    assert header == origHeader
except AssertionError:
    print((colorama.Fore.RED + 'New header is not equal!!'))
    print((colorama.Fore.BLUE) + str(origHeader))
    print((colorama.Fore.RED + str(header)))
else:
    print((colorama.Fore.GREEN + 'Header equal before and after read / write'))

print((colorama.Fore.GREEN + 'Test succeeded!'))
