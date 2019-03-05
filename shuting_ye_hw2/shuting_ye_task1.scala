import java.io._
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import shuting_ye_task2.myordering


object shuting_ye_task1 {

  def comb2(k: Int):Int = (k-1)*k/2

  /* Compare two ordered list of same size, act just like tuple comparision in python*/
  def mylt(a:List[Int], b:List[Int]):Boolean = {
    for (i<-0 until a.length){
      if (a(i) < b(i)) return true
      else if (a(i) > b(i)) return false
    }
    false
  }

  implicit val myordering = new Ordering[List[String]] {
    override def compare(x: List[String], y: List[String]): Int = {
      var ret = 0
      for (i <- 0 until x.length){
        ret = x(i).compareTo(y(i))
        if (ret!=0) return ret
      }
      ret
    }
  }
  def genLi(ci: RDD[Set[Int]], sup: Int, part: Array[Set[Int]]) =  ci.flatMap(x=> for(i <- part if x.subsetOf(i)) yield (x,1)).groupByKey().filter(x=> x._2.size>=sup).keys

  def genCi(prevl: RDD[Set[Int]], k: Int):RDD[Set[Int]] = {
    val prevl4j = prevl.map(x=>(1,x))
    val itemset = prevl4j.join(prevl4j).values.filter(x=> mylt(x._1.toList.sorted,x._2.toList.sorted))
    if (k==2) return itemset.map(x=>x._1.++(x._2)).coalesce(1)

    itemset.filter(x=> k-2==(x._1.&(x._2)).size).coalesce(1)
      .map(x=>(x._1.++(x._2),null)).groupByKey().filter(x=> comb2(k)==x._2.size).keys
  }


  def Apriori_Alg(part: RDD[Set[String]], sup: Int):List[RDD[Set[String]]] = {

    var allL = List[RDD[Set[String]]]()

    val c1 = part.flatMap(x=>x)
    val l1 = c1.map(x=>(x,null)).groupByKey().filter(x=> x._2.size>=sup).keys
    val c1_li = c1.collect().distinct.sorted
    var prevl = l1.map(x=> Set(c1_li.indexOf(x)))
    allL = allL:+(l1.map(x=>Set(x)))

    val translated_part = part.map(x=>x.map(i=>c1_li.indexOf(i))).collect()

    var k = 1
    var flag = true
    while(flag){
      k+=1
      val ci = genCi(prevl, k)
      val li = genLi(ci, sup, translated_part)
      allL = allL:+(li.map(x=> x.map(i=>c1_li(i)) ))
      prevl = li

      val len_li = li.collect().length
      if (len_li==0) flag=false

    }

    allL
  }

  def SON_Alg(baskets: RDD[Set[String]], sup: Int): (Array[List[List[String]]], List[List[List[String]]]) = {

    val numPartition = baskets.getNumPartitions
    val fraction_sup = sup / numPartition

    // pass1
    val sample = baskets.randomSplit(Array.fill(numPartition)(1.0), 10)
    var maxli = 0
    val allL = new Array[List[RDD[Set[String]]]](numPartition)
    for(i <- 0 until sample.length){
      val partL = Apriori_Alg(sample(i), fraction_sup)
      allL(i) = partL.dropRight(1)
      if(partL.length-1>allL(maxli).length) maxli = i
    }

    val maxl = allL(maxli).length
    val candidates = new Array[List[List[String]]](maxl)
    val zipres = allL(0).zip(allL(1))
    for (i<-0 until zipres.length){
      candidates(i) = zipres(i)._1.union(zipres(i)._2).map(x=>(x,null)).groupByKey().keys
        .map(x=> x.toList.sorted).collect().toList.sorted(myordering)
    }
    for (i<-zipres.length until maxl if maxl>zipres.length){
      candidates(i) = allL(maxli)(i).map(x=> x.toList.sorted).collect().toList.sorted(myordering)
    }

    // pass2
    var freqItemsets = List[List[List[String]]]()
    for(i<-0 until candidates.length){
      val fitemi = baskets.flatMap(x => for (c <- candidates(i) if c.toSet.subsetOf(x)) yield (c, null))
        .groupByKey().filter(x=> x._2.size>=sup).keys.collect()

      if(fitemi.length>0){
        freqItemsets = freqItemsets:+(fitemi.toList.sorted(myordering))
      }
    }

    (candidates, freqItemsets)
  }

  def case1(rdd: RDD[String], sup: Int): (Array[List[List[String]]], List[List[List[String]]])={

    //create baskets for each user, where business ids are unique
    val user_basks = rdd.filter(x => !x.contains("user_id")).map(x => x.split(",")).map(x => (x(0),x(1)))
      .groupByKey().map(x => x._2.toSet).persist()

    SON_Alg(user_basks, sup)
  }

  def case2(rdd: RDD[String], sup: Int): (Array[List[List[String]]], List[List[List[String]]])={

    val business_basks = rdd.filter(x => !x.contains("user_id")).map(x => x.split(",")).map(x => (x(1),x(0)))
      .groupByKey().map(x => x._2.toSet).persist()

    SON_Alg(business_basks, sup)
  }

  def main(args: Array[String]): Unit = {

    val opt = args(0).toInt
    val sup = args(1).toInt
    val infile = args(2)
    val outfile = args(3)

    val ss = SparkSession.builder().appName("task1").config("spark.master","local[*]")
      .getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("WARN")

    val start = new Date().getTime
    val smallRDD = sc.textFile(infile)

    val (candidates, freqItemsets) = if (opt==1) case1(smallRDD, sup) else case2(smallRDD, sup)

    val outf = new FileWriter(new File(outfile))
    outf.write("Candidates:")
    for (can <- candidates){
      outf.write("\n" + can.map(x=> x.mkString("('", "', '", "')")).mkString(",") + "\n")
    }
    outf.write("\nFrequent Itemsets:")
    for (i <- 0 until freqItemsets.length){
      if (i<freqItemsets.length-1)
        outf.write("\n" + freqItemsets(i).map(x=> x.mkString("('", "', '", "')")).mkString(",")+"\n")
      else
        outf.write("\n" + freqItemsets(i).map(x=> x.mkString("('", "', '", "')")).mkString(","))
    }
    outf.close()

    val end = new Date().getTime
    println("Duration:"+(end-start)/1000)
  }

}