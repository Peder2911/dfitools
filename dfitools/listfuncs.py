def chunk(l,n):
    # Snipped from SO
    # splits a list (l) into a list of chunks of (n) length
    for i in range(0,len(l),n):
        yield l[i:i +n]


