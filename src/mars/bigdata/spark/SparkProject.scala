package mars.bigdata.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ stddev_samp, stddev_pop }
import scala.collection.immutable._
import scala.io.Source

object SparkProject extends App {

  override def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val filename = "../mtcars.csv";

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("NormalDistribution")
      .master("local[*]")
      .getOrCreate()
    // Convert to a DataSet
    import spark.implicits._

    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
    val headerAndRows = spark.sparkContext.textFile(filename)
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_ != header)
    val carsDS = mtcdata.map(mapper).toDS().cache() //since we will run the dataset more than one, we will use cache so that we don't have reconstruct it multiple times
    println("Here is our inferred schema:")
    carsDS.printSchema()
    var catList = loadCategoryNames(filename)
    println(catList)
    val category_name = "cyl"
    val mean_column = "mpg"
    val sampleCache = carsDS.sample(false, .25).cache()
    val the_sample = sampleCache.groupBy(category_name).agg(avg(mean_column), variance(mean_column)).cache()
    println("Printing the sample:")
    the_sample.show()
    val resampledData = sampleCache;
    val sample = the_sample.collect()
    var catMap: Map[String, Pair] = Map() //sample(0)(0) -> (sample(0)(1), sample(0)(2)))
    val repeats = 10;
    //for each category
    println("Step 5. Do 1000 times for each category")
    for (cat <- catList) {
      for (x <- 1 to repeats) {
        var temp = sampleCache.sample(true, 1).filter(col(category_name).equalTo(cat)).groupBy(category_name)
          .agg(avg(mean_column), variance(mean_column))
          .collect()
        for (r: Row <- temp) {
          val rcat = r(0).asInstanceOf[Double].toString()
          val rmean = r(1).asInstanceOf[Double]
          val rvar = r(2).asInstanceOf[Double]
          val stat = Stat(rcat, Pair(rmean, rvar))
          println(stat)
          val category = stat.category
          val current = stat.pair
          var updated = Pair(current.mean, current.variance)
          println(updated)
          var prev = catMap.getOrElse(category, null)
          //val prev = catMap(category).asInstanceOf[Pair]
          if (prev != null) {
            println("found in hashmap")
            //catMap. -=(category)
            updated = Pair(prev.mean + current.mean, prev.variance + current.variance)
          }
          catMap += (category -> updated)
        }
      }

    }
    println("Printing hashmap")
    catMap.foreach(println)
    // Some SQL-style magic to sort all movies by popularity in one line!

    //sampleCache.show()
    // Stop the session
    spark.stop()

  }
  // Case class so we can get a column name for our movie ID
  case class Car(mpg: Double, cyl: Double)
  case class Stat(category: String, pair: Pair)
  case class Pair(mean: Double, variance: Double)

  def mapper(line: String): Car = {
    val fields = line.split(',')
    val c: Car = Car(fields(1).toDouble, fields(2).toInt)
    //println(c)
    return c
  }

  //  def updateSample(arecord, prev: Stat): Stat = {
  //    val category = arecord(0)
  //    val mean = arecord(1)
  //    val variance = arecord(2)
  //    var current: Stat = Stat(mean, variance)
  //    if (prev != null)
  //      current = Stat(prev._1 + mean, prev._2 + variance)
  //    return current
  //  }
  def loadCategoryNames(filename: String): List[String] = {

    var catNames: HashSet[String] = HashSet()

    val lines = Source.fromFile(filename).getLines()
    for (line <- lines) {
      var fields = line.split(',')
      if (fields.length > 1) {
        catNames += fields(2).toString()
      }
    }

    return catNames.toList
  }
}