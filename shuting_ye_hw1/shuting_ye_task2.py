#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
import json
from pyspark import SparkConf, SparkContext
import datetime
import re


def getBusiness_id(x):
    business_id = re.compile('"business_id":"([a-zA-Z0-9_-]+)"')
    return business_id.search(x).group(1)

def cusPart(k):
    return hash(k)


def task2(argv):
    infile, outfile, n_partition = argv[1], argv[2], int(argv[3])

    sc = SparkContext('local[*]','task2', 
        conf=SparkConf().set('spark.driver.memory','4g').set('spark.executor.memory','4g'))

    reviewRDD = sc.textFile(infile)

    #
    # default
    def_start = datetime.datetime.now()

    reviewRDD.map(getBusiness_id).map(lambda w: [w,1]).reduceByKey(lambda x,y: x+y)\
        .sortBy(lambda x: x[1], False).take(10)

    def_end = datetime.datetime.now()

    # number of partitions of the default partition
    def_n_partiotion = reviewRDD.getNumPartitions()
    # number of items in each partition
    def_n_items = reviewRDD.mapPartitions(lambda p: [len(list(p))]).collect()
    # execution time of the default partition
    def_exe_time = (def_end - def_start).seconds

    #
    # customized
    cus_start = datetime.datetime.now()

    reviewRDD2 = reviewRDD.map(getBusiness_id).map(lambda w: [w,1]).partitionBy(n_partition, cusPart)
    reviewRDD2.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(10)

    cus_end = datetime.datetime.now()

    # number of partitions of the customized partition
    cus_n_partiotion = reviewRDD2.getNumPartitions()
    # number of items in each partition
    cus_n_items = reviewRDD2.mapPartitions(lambda p: [len(list(p))]).collect()
    # execution time of the default partition
    cus_exe_time = (cus_end - cus_start).seconds

    result = {
        'default':{'n_partition':def_n_partiotion, 'n_items':def_n_items, 'exe_time':def_exe_time},
        'customized':{'n_partition':cus_n_partiotion, 'n_items':cus_n_items, 'exe_time':cus_exe_time},
        'explanation':'Too small number of partitions will make some wroker being idle which is a '+
            'waste of resource, and too large number of partitions will make many tasks pending, '+
            'leading to the increase of the overall execution time. Besides, proper partition function '+
            'will reduce the shuffle and communication time.'}

    jsonResult = json.dumps(result)

    with open(outfile, 'w') as outf:
        outf.write(jsonResult)


if __name__ == '__main__':
    task2(sys.argv)