import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization
import java.io._



object shuting_ye_task1 {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
      .set("spark.executor.memory", "4g").set("spark.driver.memory","4g")
    val ss = SparkSession.builder().config(conf)
      .getOrCreate()
    val sc = ss.sparkContext

    val reviewRDD = sc.textFile(infile)

    // The total number of reviews
    val n_review = reviewRDD.count()

    // The number of reviews in 2018
    val n_review_2018 = reviewRDD.filter(s => s.contains("\"date\":\"2018-")).count()
    
    // The number of distinct users who wrote reviews (1637138)
    val n_user = reviewRDD.map(getUser_id).distinct().count()

    // The top 10 users who wrote the largest numbers of reviews
    // and the number of reviews they wrote
    val top10_user = reviewRDD.map(getUser_id).map(w => (w,1)).reduceByKey(_+_)
        .sortBy(_._2, false).take(10)

    // The number of distinct businesses that have been reviewed (192606)
    val n_business = reviewRDD.map(getBusiness_id).distinct().count()

    // The top 10 businesses that had the largest numbers of reviews
    // and the number of reviews they had
    val top10_business = reviewRDD.map(getBusiness_id).map(w => (w,1)).reduceByKey(_+_)
        .sortBy(_._2, false).take(10)

    val top10_user1 = new Array[List[Any]](top10_user.length)
    for(i <- 0 until top10_user.length) top10_user1(i)=List(top10_user(i)._1,top10_user(i)._2)

    val top10_business1 = new Array[List[Any]](top10_business.length)
    for(i <- 0 until top10_business.length) top10_business1(i)=List(top10_business(i)._1,top10_business(i)._2)

    implicit val formats = DefaultFormats
    val result = Map(("n_review"->n_review), ("n_review_2018"->n_review_2018), ("n_user"->n_user),
      ("top10_user"->top10_user1.toList), ("n_business"->n_business), ("top10_business"->top10_business1.toList))

    val jsonResult = Serialization.write(result)

    val outf = new FileWriter(new File(outfile))
    outf.write(jsonResult)
    outf.close()
  }
}
