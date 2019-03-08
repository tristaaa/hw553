#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
from pyspark import SparkContext
from datetime import datetime
<<<<<<< HEAD
from itertools import combinations

def myhash1(x,a,b,m):
    """f(x)= (ax + b) % m"""
    return (a*x+b)%m 

def myhash2(x,a,b,p,m):
    """f(x) = ((ax + b) % p) % m"""
    return ((a*x+b)%p)%m 

def gen_hashed_rows(x,m):
    permutead_rows = []
    a_grp = [2,10,14,22,]
    b_grp = 
    p_grp = [3]

    hv_grp = 

def LSH_Jaccard(rdd):
    preparedRDD = rdd.filter(lambda x: 'user_id' not in x).map(lambda x:x.split(',')).persist()
    users = preparedRDD.map(lambda x: (x[0],None)).groupByKey().keys().collect()
    buss = preparedRDD.map(lambda x: (x[1],None)).groupByKey().keys().collect()
    user_num = len(users)

    utl_matrix = preparedRDD.map(lambda x: (buss.index(x[1], users.index(x[0])))).groupByKey().mapValues(list)



=======

def myhash1(x):
    """f(x)= (ax + b) % m"""
    pass 

def myhash2(x):
    """f(x) = ((ax + b) % p) % m"""
    pass 
>>>>>>> 889bdc29f8b1d9014a928cd1ab8a510b2414d8ad


def task1(argv):
    infile, outfile = argv[1], argv[2]

    sc = SparkContext('local[*]','task1')

    start = datetime.now()
    trainRDD = sc.textFile(infile)

<<<<<<< HEAD
    candidates = LSH_Jaccard(trainRDD)

=======
>>>>>>> 889bdc29f8b1d9014a928cd1ab8a510b2414d8ad
    end = datetime.now()
    print('Duration:', (end-start).seconds)

if __name__ == '__main__':
    task1(sys.argv)