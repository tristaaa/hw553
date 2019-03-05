import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.io._
import java.util.Date

object shuting_ye_task2 {

  def getUser_id(s: String): String = {
    val user_id = """"user_id":"([a-zA-Z0-9_-]+)"""".r
    return (user_id findFirstMatchIn s).get.group(1)
  }

  def getBusiness_id(s: String): String = {
    val business_id = """"business_id":"([a-zA-Z0-9_-]+)"""".r
    return (business_id findFirstMatchIn s).get.group(1)
  }

  def main(args: Array[String]): Unit = {

    val infile = args(0)
    val outfile = args(1)
    val n_partition = args(2).toInt

    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
      .set("spark.executor.memory", "4g").set("spark.driver.memory","4g")
    val ss = SparkSession.builder().config(conf)
      .getOrCreate()
    val sc = ss.sparkContext

    val reviewRDD = sc.textFile(infile)

    // default
    val def_start = new Date().getTime

    reviewRDD.map(getBusiness_id).map(w => (w,1)).reduceByKey(_+_)
      .sortBy(_._2, false).take(10)

    val def_end = new Date().getTime

    // number of partitions of the default partition
    val def_n_partiotion = reviewRDD.getNumPartitions
    // number of items in each partition
    val  def_n_items = reviewRDD.mapPartitions(p => Iterator(p.length)).collect()
    // execution time of the default partition
    val  def_exe_time = (def_end - def_start)/1000

    //
    // customized
    val cus_start = new Date().getTime

    val reviewRDD2 = reviewRDD.map(getBusiness_id).map(w => (w,1)).partitionBy(new HashPartitioner(n_partition))
    reviewRDD2.reduceByKey(_+_).sortBy(_._2, false).take(10)

    val cus_end = new Date().getTime

    // number of partitions of the customized partition
    val cus_n_partiotion = reviewRDD2.getNumPartitions
    // number of items in each partition
    val cus_n_items = reviewRDD2.mapPartitions(p => Iterator(p.length)).collect()
    // execution time of the default partition
    val cus_exe_time = (cus_end - cus_start)/1000

    val result = ("default"->(("n_partition"->def_n_partiotion)~ ("n_items"->def_n_items.toList)~ ("exe_time"->def_exe_time)))~
      ("customized"->(("n_partition"->cus_n_partiotion)~ ("n_items"->cus_n_items.toList)~ ("exe_time"->cus_exe_time)))~
      ("explanation"->("Too small number of partitions will make some wroker being idle which is a "+
        "waste of resource, and too large number of partitions will make many tasks pending, "+
        "leading to the increase of the overall execution time. Besides, proper partition function "+
        "will reduce the shuffle and communication time."))

    val jsonResult = compact(render(result))

    val outf = new FileWriter(new File(outfile))
    outf.write(jsonResult)
    outf.close()
  }
}
