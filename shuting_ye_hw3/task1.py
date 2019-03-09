#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
from pyspark import SparkContext
from datetime import datetime

def myhash1(x,a,b,m):
    """f(x)= (ax + b) % m"""
    return (a*x+b)%m 

def myhash2(x,a,b,p,m):
    """f(x) = ((ax + b) % p) % m"""
    return ((a*x+b)%p)%m 

def minhash(arr,m):
    a_grp = [17,107,387,691,879,1559,2783] #7
    b_grp = range(337,7733,1256) #6
    p_grp = [97,347,541,773,919] #5
    return [min([myhash2(x,a,b,p,m) for x in arr]) for a in a_grp for b in b_grp for p in p_grp]

def LSH_Jaccard(rdd):
    preparedRDD = rdd.filter(lambda x: 'user_id' not in x).map(lambda x:x.split(',')).persist()
    userdict = dict(preparedRDD.map(lambda x: (x[0],None)).groupByKey().sortByKey().keys().zipWithIndex().collect())
    busdict = dict(preparedRDD.map(lambda x: (x[1],None)).groupByKey().sortByKey().keys().zipWithIndex().collect())
    user = list(userdict.keys())
    bus = list(busdict.keys())
    usernum = len(user) #11270
    # busnum = len(bus)

    charmat = preparedRDD.map(lambda x: (busdict.get(x[1]), userdict.get(x[0]))).groupByKey().mapValues(list)
    sigmat = charmat.mapValues(lambda x: minhash(x,usernum))

    n = 210
    b = 21
    r = 10
    sliced_sigmat = sigmat.flatMap(lambda x: [(i//r,(x[0],x[1][i:i+r])) for i in range(0,n,r)])\
        .groupByKey().values().map(list)





def task1(argv):
    infile, outfile = argv[1], argv[2]

    sc = SparkContext('local[*]','task1')

    start = datetime.now()
    trainRDD = sc.textFile(infile)

    LSH_Jaccard(trainRDD)

    end = datetime.now()
    print('Duration:', (end-start).seconds)

if __name__ == '__main__':
    task1(sys.argv)