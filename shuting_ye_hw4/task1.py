import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from datetime import datetime
from itertools import combinations
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *


def constructSN(thre, rdd):
    preparedRDD = rdd.filter(lambda x: 'user_id' not in x).map(lambda x:x.split(',')).persist()
    userdict = dict(preparedRDD.map(lambda x: (x[0],None)).groupByKey().sortByKey().keys().zipWithIndex().collect())
    busdict = dict(preparedRDD.map(lambda x: (x[1],None)).groupByKey().sortByKey().keys().zipWithIndex().collect())
    user = list(userdict.keys())
    bus = list(busdict.keys())
    usernum = len(user)

    translatedRDD = preparedRDD.map(lambda x: (userdict[x[0]], busdict[x[1]]))
    ubdict = dict(translatedRDD.groupByKey().mapValues(set).collect())
    userpair = list(combinations(range(usernum),2))

def task1(argv):
    thre, infile, outfile = int(argv[1]), argv[2], argv[3]

    sc = SparkContext('local[*]','task1')

    start = datetime.now()
    ubRDD = sc.textFile(infile)

    sng = constructSN(thre, ubRDD)

    end = datetime.now()
    print('Duration:', (end-start).seconds)

if __name__ == '__main__':
    task1(sys.argv)