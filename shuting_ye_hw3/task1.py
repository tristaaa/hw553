#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
from pyspark import SparkContext
from datetime import datetime

def myhash1(x):
    """f(x)= (ax + b) % m"""
    pass 

def myhash2(x):
    """f(x) = ((ax + b) % p) % m"""
    pass 


def task1(argv):
    infile, outfile = argv[1], argv[2]

    sc = SparkContext('local[*]','task1')

    start = datetime.now()
    trainRDD = sc.textFile(infile)

    end = datetime.now()
    print('Duration:', (end-start).seconds)

if __name__ == '__main__':
    task1(sys.argv)