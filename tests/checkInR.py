
import subprocess

def checkInR(datastring):
    p = subprocess.run('tests/peruse.R',input = datastring.encode(),stdout = subprocess.PIPE)
    results = [int(ln) for ln in p.stdout.decode().splitlines()]
    return({'rows':results[0],'columns':results[1]})
    
