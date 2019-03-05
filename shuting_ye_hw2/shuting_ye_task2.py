#! /usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
from pyspark import SparkContext
from datetime import datetime

def myHash(i,j,B):
    """i, j are translated integer for each item"""
    return (-i+j) % B

def comb2(n):
    return (n-1)*n//2

def genTup(x):
    """x[0] and x[1] are already ordered"""
    item = tuple(set(x[1])-set(x[0]))
    idx = list(x[1]).index(item[0])+1
    return x[0][:idx]+item+x[0][idx:]


def in_freqbuck(x, bucksize, bitmap):
    hv = myHash(x[0],x[1],bucksize)
    return bitmap[hv//31] & (1<<(hv%31))

def genLi(ci, sup, part):
    """find truely frequent iemset"""
    return part.flatMap(lambda x:[(i,None) for i in ci if set(i)<=x]).groupByKey().filter(lambda x: len(x[1])>=sup).keys()

def genCi(prevl, k, bitmap=None, bucksize=0):
    """
    prevl: rdd of truely freq. itemset of size k-1
    bitmap: useful to filter the c2 
    return : itemset is a rdd of list of tuples
    a candidate must satify that all its immediate subset are freq. itemset
        which means its combination must have size of k(k-1)/2
    """
    prevl4j = prevl.map(lambda x:(None,x))
    itemset = prevl4j.join(prevl4j).map(lambda x:x[1]).filter(lambda x: x[0]<x[1])
    if k>2:
        return itemset.filter(lambda x: k-2==len(set(x[0])&set(x[1]))).coalesce(1).map(genTup).map(lambda x:(x,None)).groupByKey().filter(lambda x: comb2(k)==len(x[1])).keys()
    else:
        # pair in c2 must also hash to a freq. bucket
        return itemset.filter(lambda x: in_freqbuck(x,bucksize,bitmap)).coalesce(1)

   
def PCY_Alg(part, sup):
    """
    input: 
        part: rdd of one sample of the entire rdd
    return:
        list of all sizes L (rdd)
    """

    # pass one: output frequent singletons and frequent buckets
    allL=[]
    bucksize = 26131
    bitmap = [0]*((bucksize+30)//31)

    c1 = part.flatMap(lambda x:x[1])
    l1 = c1.map(lambda x:(x,None)).groupByKey().filter(lambda x: len(x[1])>=sup).keys()
    c1_li = sorted(set(c1.collect()))
    prevl = l1.map(lambda x: c1_li.index(x))
    allL.append(l1.map(lambda x:(x,)))

    # freq. buckets
    users = part.keys().collect()
    # renumber the items, str to integer
    translated_part = part.map(lambda x:(users.index(x[0]),set([c1_li.index(i) for i in x[1]])))
    flattp = translated_part.flatMapValues(lambda x: x)
    freqbuck = flattp.join(flattp).values().filter(lambda x:x[0]<x[1])\
        .map(lambda x: myHash(x[0],x[1],bucksize)).map(lambda x:(x,None)).groupByKey()\
        .filter(lambda x: len(x[1])>=sup).keys().collect()
    for i in freqbuck:
        bitmap[i//31] |= 1<<(i%31) # set the bit to 1


    k = 1
    while True:
        k+=1
        ci=[]
        if k==2:
            ci = genCi(prevl, k, bitmap=bitmap, bucksize=bucksize)
        else:
            ci = genCi(prevl, k)

        ci_li=ci.collect()
        li = genLi(ci_li, sup, translated_part.values())
        len_li = len(li.collect())
        if len_li==0:
            break

        allL.append(li.map(lambda x: tuple([c1_li[i] for i in x])))
        prevl = li


    return allL


def SON_Alg(baskets, sup, eptRDD):
    """
    First Map: 
        input: one partition of baskets, sup = sup/numPartition
        output: (F,None) where F can be one of all sizes of freq. itemset
    Fist Reduce:
        eliminate the duplicates
    Second Map:
        local count for each freq. itemsets
    Second Reduce:
        only output itemsets whose count >= sup 
    """
    numPartition = 2
    # numPartition = baskets.getNumPartitions()
    fraction_sup = sup//numPartition

    # pass1 
    sample = baskets.randomSplit([1]*numPartition)
    maxl,allL=0,[]
    for part in sample:
        partL = PCY_Alg(part, fraction_sup)
        if len(partL)>maxl:
            maxl=len(partL)
        allL.append(partL)
    
    candidates=[]
    for i in range(maxl):
        rddLi = eptRDD
        for np in range(numPartition):
            try:
                rddLi = rddLi.union(allL[np][i])
            except IndexError:
                continue
        candidates.append(sorted(rddLi.map(lambda x: (x,None)).groupByKey().keys().collect()))


    # pass2
    freqItemsets = []
    for can in candidates:
        fitemi = baskets.flatMap(lambda x:[(i,None) for i in can if set(i)<=x[1]]).groupByKey().filter(lambda x: len(x[1])>=sup).keys().collect()
        if len(fitemi)>0:
            freqItemsets.append(sorted(fitemi))

    return (candidates, freqItemsets)


def case1(rdd, thre, sup, eptRDD):

    # create baskets for each user, where business ids are unique
    user_basks = rdd.filter(lambda x: 'user_id' not in x).map(lambda x:x.split(','))\
        .groupByKey().map(lambda x: (x[0],set(x[1]))).filter(lambda x: len(x[1])>thre).persist()

    return SON_Alg(user_basks, sup, eptRDD)    


def task2(argv):
    thre, sup, infile, outfile = int(argv[1]), int(argv[2]), argv[3], argv[4]

    sc = SparkContext('local[*]','task2')

    start = datetime.now()
    ubRDD = sc.textFile(infile)
    eptRDD = sc.parallelize([])

    candidates, freqItemsets=case1(ubRDD, thre, sup, eptRDD)

    with open(outfile,'w') as outf:
        outf.write('Candidates:')
        for can in candidates:
            if can==candidates[0]:
                outf.write('\n'+','.join(str(c)[:-2]+str(c)[-1] for c in can))
            else:
                outf.write('\n\n'+','.join(str(c) for c in can))

        outf.write('\n\nFrequent Itemsets:')
        for fi in freqItemsets:
            if fi==freqItemsets[0]:
                outf.write('\n'+','.join(str(f)[:-2]+str(f)[-1] for f in fi))
            else:
                outf.write('\n\n'+','.join(str(f) for f in fi))


    end = datetime.now()
    print('Duration:', (end-start).seconds)

if __name__ == '__main__':
    task2(sys.argv)