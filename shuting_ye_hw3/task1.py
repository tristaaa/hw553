#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
from pyspark import SparkContext
from datetime import datetime


def myhash(x,a,b,p,m):
    """f(x) = ((ax + b) % p) % m"""
    return ((a*x+b)%p)%m 

def minhash(arr,m):
    a_grp = [17,89,387,691,1559,2783,3773,4356] #8
    b_grp = range(3337,7733,788) #6
    p_grp = [541,691,797,977,1311] #5
    return [min([myhash(x,a,b,p,m) for x in arr]) for a in a_grp for b in b_grp for p in p_grp]

def genpair(x):
    for i in range(len(x)-1):
        for j in range(i+1,len(x)):
            if x[i][1]==x[j][1]:
                yield (x[i][0],x[j][0])
            else:
                break

def LSH_Jaccard(rdd):
    preparedRDD = rdd.filter(lambda x: 'user_id' not in x).map(lambda x:x.split(',')).persist()
    userdict = dict(preparedRDD.map(lambda x: (x[0],None)).groupByKey().sortByKey().keys().zipWithIndex().collect())
    busdict = dict(preparedRDD.map(lambda x: (x[1],None)).groupByKey().sortByKey().keys().zipWithIndex().collect())
    bus = list(busdict.keys())
    usernum = len(userdict.keys()) #11270

    charmat = preparedRDD.map(lambda x: (busdict[x[1]], userdict[x[0]])).groupByKey().mapValues(set)
    sigmat = charmat.mapValues(lambda x: minhash(x,usernum))

    # the recall should >=0.9, so (1-0.5^r)^b<=0.1
    # if r=5, b should >=73; if r=4, b should >=36
    n = 8*6*5
    r = 4
    sliced_sigmat = sigmat.flatMap(lambda x: [(i//r,(x[0],x[1][i:i+r])) for i in range(0,n,r)]).groupByKey().values().map(list)
    cand = sliced_sigmat.map(lambda x: sorted(x,key=lambda x: (x[1],x[0]))).flatMap(lambda x: genpair(x))
    charmatdict = dict(charmat.collect())

    candidates = cand.map(lambda x: (x[0],x[1], len(charmatdict[x[0]]&charmatdict[x[1]])/len(charmatdict[x[0]]|charmatdict[x[1]])))\
        .filter(lambda x:x[2]>=0.5).map(lambda x: (x, None)).groupByKey().keys()

    return candidates.map(lambda x: (bus[x[0]], bus[x[1]], str(x[2])))

def task1(argv):
    infile, outfile = argv[1], argv[2]

    sc = SparkContext('local[*]','task1')

    start = datetime.now()
    trainRDD = sc.textFile(infile)

    candidates = LSH_Jaccard(trainRDD)

    with open(outfile,'w') as outf:
        outf.write('business_id_1, business_id_2, similarity')
        for can in sorted(candidates.collect()):
            outf.write('\n'+",".join(can))

    end = datetime.now()
    print('Duration:', (end-start).seconds)

if __name__ == '__main__':
    task1(sys.argv)