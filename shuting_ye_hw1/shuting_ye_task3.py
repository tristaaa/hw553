#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
import json
from pyspark import SparkConf, SparkContext
import re
import datetime


def getBus_idAndStars(x):
    business_id = re.compile('"business_id":"([a-zA-Z0-9_-]+)"')
    stars = re.compile('"stars":([0-9]+[.][0-9]+)')
    return (business_id.search(x).group(1), float(stars.search(x).group(1)))

def getBus_idAndCity(x):
    business_id = re.compile('"business_id":"([a-zA-Z0-9_-]+)"')
    city = re.compile('"city":"(.*)","state"')
    return (business_id.search(x).group(1), city.search(x).group(1))


def task3(argv):
    infile1, infile2, outfile1, outfile2 = argv[1], argv[2], argv[3], argv[4]

    sc = SparkContext('local[*]','task3', 
        conf=SparkConf().set('spark.driver.memory','4g').set('spark.executor.memory','4g'))


    reviewRDD = sc.textFile(infile1)
    businessRDD = sc.textFile(infile2)

    #
    # average stars for each city

    businessRDD1 = businessRDD.map(getBus_idAndCity)
    n_part = businessRDD1.getNumPartitions()

    # repartition first to reduce the shuffle time
    reviewRDD1 = reviewRDD.map(getBus_idAndStars).partitionBy(n_part)
    
    avgStars = reviewRDD1.join(businessRDD1).map(lambda x: (x[1][1],x[1][0]))\
        .aggregateByKey((0,0),lambda u,x:(u[0]+x,u[1]+1),lambda u,x:(u[0]+x[0],u[1]+x[1]))\
        .map(lambda x: (x[1][0]/x[1][1],x[0])).groupByKey().sortByKey(False)\
        .mapValues(lambda v: sorted(v)).collect()

    with open(outfile1,'w') as outf:
        outf.write('city,stars')
        for k,v in avgStars:
            for city in v:
                outf.write('\n'+city+','+str(k))

    # use two ways to print top 10 cities with highest stars
    # Method1: Collect all the data, and then print the first 10 cities
    m1_start = datetime.datetime.now()

    m1_top10 = reviewRDD1.join(businessRDD1).map(lambda x: (x[1][1],x[1][0]))\
        .aggregateByKey((0,0),lambda u,x:(u[0]+x,u[1]+1),lambda u,x:(u[0]+x[0],u[1]+x[1]))\
        .map(lambda x: (x[1][0]/x[1][1],x[0])).groupByKey().sortByKey(False)\
        .flatMap(lambda x:sorted(x[1])).collect()

    print("m1_top10_city:", m1_top10[:10])

    m1_end = datetime.datetime.now()
    m1_exe_time = (m1_end - m1_start).seconds

    # Method2: Take the first 10 cities, and then print all
    m2_start = datetime.datetime.now()

    m2_top10 = reviewRDD1.join(businessRDD1).map(lambda x: (x[1][1],x[1][0]))\
        .aggregateByKey((0,0),lambda u,x:(u[0]+x,u[1]+1),lambda u,x:(u[0]+x[0],u[1]+x[1]))\
        .map(lambda x: (x[1][0]/x[1][1],x[0])).groupByKey().sortByKey(False)\
        .flatMap(lambda x:sorted(x[1])).take(10)

    print("m2_top10_city:", m2_top10)

    m2_end = datetime.datetime.now()
    m2_exe_time = (m2_end - m2_start).seconds

    result = {
        'm1': m1_exe_time,'m2':m2_exe_time,
        'explanation':'The second method use take() instead of collect(), which is more complex, '+
            'it will first look at one partition and use the result to estimate the number of partitions '+
            'needed for the rest of the work, while collect just calculate all the result and combine them.'}

    jsonResult = json.dumps(result)

    with open(outfile2, 'w') as outf:
        outf.write(jsonResult)


if __name__ == '__main__':
    task3(sys.argv)