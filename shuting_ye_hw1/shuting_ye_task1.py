#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
import json
from pyspark import SparkConf, SparkContext
import re

def getUser_id(x):
    user_id = re.compile('"user_id":"([a-zA-Z0-9_-]+)"')
    return user_id.search(x).group(1)

def getBusiness_id(x):
    business_id = re.compile('"business_id":"([a-zA-Z0-9_-]+)"')
    return business_id.search(x).group(1)

def task1(argv):
    infile, outfile = argv[1], argv[2]

    sc = SparkContext('local[*]','task1', 
        conf=SparkConf().set('spark.driver.memory','4g').set('spark.executor.memory','4g'))

    reviewRDD = sc.textFile(infile)

    # The total number of reviews
    n_review = reviewRDD.count()

    # The number of reviews in 2018
    n_review_2018 = reviewRDD.filter(lambda x: '"date":"2018-' in x).count()
    
    # The number of distinct users who wrote reviews (1637138)
    n_user = reviewRDD.map(getUser_id).distinct().count()

    # The top 10 users who wrote the largest numbers of reviews 
    # and the number of reviews they wrote
    top10_user = reviewRDD.map(getUser_id).map(lambda w: [w,1]).reduceByKey(lambda x,y: x+y)\
        .sortBy(lambda x: x[1], False).take(10)

    # The number of distinct businesses that have been reviewed (192606)
    n_business = reviewRDD.map(getBusiness_id).distinct().count()

    # The top 10 businesses that had the largest numbers of reviews 
    # and the number of reviews they had
    top10_business = reviewRDD.map(getBusiness_id).map(lambda w: [w,1]).reduceByKey(lambda x,y: x+y)\
        .sortBy(lambda x: x[1], False).take(10)
        
    result = {'n_review':n_review, 'n_review_2018':n_review_2018, 'n_user':n_user,
        'top10_user':top10_user, 'n_business':n_business, 'top10_business':top10_business}

    jsonResult = json.dumps(result)

    with open(outfile, 'w') as outf:
        outf.write(jsonResult)

if __name__ == '__main__':
    task1(sys.argv)