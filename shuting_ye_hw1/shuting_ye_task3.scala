import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.io._
import java.util.Date

object shuting_ye_task3 {

  def getBus_idAndStars(s: String): (String, String) = {
    val business_id = """"business_id":"([a-zA-Z0-9_-]+)"""".r
    val stars = """"stars":([0-9]+[.][0-9]+)""".r
    return ((business_id findFirstMatchIn s).get.group(1), (stars findFirstMatchIn s).get.group(1))
  }

  def getBus_idAndCity(s: String): (String, String) = {
    val business_id = """"business_id":"([a-zA-Z0-9_-]+)"""".r
    val city = """"city":"(.*)","state"""".r
    return ((business_id findFirstMatchIn s).get.group(1), (city findFirstMatchIn s).get.group(1))
  }

  def main(args: Array[String]): Unit = {

    val infile1 = args(0)
    val infile2 = args(1)
    val outfile1 = args(2)
    val outfile2 = args(3)

    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
      .set("spark.executor.memory", "4g").set("spark.driver.memory","4g")
    val ss = SparkSession.builder().config(conf)
      .getOrCreate()
    val sc = ss.sparkContext

    val reviewRDD = sc.textFile(infile1)
    val businessRDD = sc.textFile(infile2)

    //
    //average stars for each city
    val businessRDD1 = businessRDD.map(getBus_idAndCity)
    val n_part = businessRDD1.getNumPartitions

//    repartition first to reduce the shuffle time
    val reviewRDD1 = reviewRDD.map(getBus_idAndStars).partitionBy(new HashPartitioner(n_part))

    val avgStars = reviewRDD1.join(businessRDD1).map(x=> (x._2._2,x._2._1))
      .aggregateByKey((0.0,0.0))((x,y)=>(x._1+y.toString.toFloat,x._2+1),(x,y)=>(x._1+y._1,x._2+y._2))
      .map(x => (x._2._1/x._2._2,x._1)).groupByKey().sortByKey(false)
      .mapValues(v => v.toList.sorted).collect()

    val outf1 = new FileWriter(new File(outfile1))
    outf1.write("city,stars")
    avgStars.foreach( v => {
      v._2.foreach(city => {
        outf1.write("\n"+city+","+v._1.toString)
      })
    })
    outf1.close()


    // use two ways to print top 10 cities with highest stars
    // Method1: Collect all the data, and then print the first 10 cities
    val m1_start = new Date().getTime

    val m1_top10 = reviewRDD1.join(businessRDD1).map(x=> (x._2._2,x._2._1))
      .aggregateByKey((0.0,0.0))((x,y)=>(x._1+y.toString.toFloat,x._2+1),(x,y)=>(x._1+y._1,x._2+y._2))
      .map(x => (x._2._1/x._2._2,x._1)).groupByKey().sortByKey(false)
      .flatMap(x => x._2.toList.sorted).collect()

    println("m1_top10_city:"+m1_top10.toList.take(10))

    val m1_end = new Date().getTime
    val m1_exe_time = (m1_end - m1_start)/1000

    // Method2: Take the first 10 cities, and then print all
    val m2_start = new Date().getTime

    val m2_top10 = reviewRDD1.join(businessRDD1).map(x=> (x._2._2,x._2._1))
      .aggregateByKey((0.0,0.0))((x,y)=>(x._1+y.toString.toFloat,x._2+1),(x,y)=>(x._1+y._1,x._2+y._2))
      .map(x => (x._2._1/x._2._2,x._1)).groupByKey().sortByKey(false)
      .flatMap(x => x._2.toList.sorted).take(10)

    println("m2_top10_city:"+m2_top10.toList)

    val m2_end = new Date().getTime
    val m2_exe_time = (m2_end - m2_start)/1000

    val result = ("m1"->m1_exe_time)~ ("m2"->m2_exe_time)~
      ("explanation"->("The second method use take() instead of collect(), which is more complex, "+
        "it will first look at one partition and use the result to estimate the number of partitions "+
        "needed for the rest of the work, while collect just calculate all the result and combine them."))

    val jsonResult = compact(render(result))

    val outf2 = new FileWriter(new File(outfile2))
    outf2.write(jsonResult)
    outf2.close()
  }
}
