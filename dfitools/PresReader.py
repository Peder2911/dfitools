import re
import sys

def get(file):
    resDict = {}

    with open(file,encoding = 'utf-8') as presfile:
        lines = presfile.readlines()
    
    lines = lines[1:]
    for line in lines:
        label = re.search('^\w+(?= - )',line)
        if label:
            labelPhrase = '^{} - '.format(label.group(0))
            regexp = re.search('(?<={}).*'.format(labelPhrase),line)
            resDict[label.group(0)] = regexp.group(0)
        else:
            pass

    return(resDict)

if __name__ == '__main__':
    presfile = sys.argv[1]
    regexps = get(presfile)

    for label,regexp in regexps.items():
        print('Trying to compile %s'%(label))
        re.compile(regexp)
        print(regexp)
        print('success!!\n')

