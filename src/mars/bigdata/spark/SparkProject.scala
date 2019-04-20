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
  val dev_mode: Boolean = true;
  override def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Initializing arguments
    val filename = "../mtcars.csv";
    val category_name = "cyl"
    val mean_column = "mpg"
    val catpos = 2 //category position in the file
    val percentage = .25
    var catMap: Map[String, Pair] = Map() //sample(0)(0) -> (sample(0)(1), sample(0)(2)))
    val repeats = 5;

    println("Step 1. The following file is selected:")
    println(filename)

    println("Step 2. The following columns are selected as the key-value pair:")
    println(category_name, mean_column)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("NormalDistribution")
      .master("local[*]")
      .getOrCreate()
    // Convert to a DataSet
    import spark.implicits._

    val headerAndRows = spark.sparkContext.textFile(filename)
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_ != header)
    val carsDS = mtcdata.map(mapper).toDS().cache() //since we will run the dataset more than one, we will use cache so that we don't have reconstruct it multiple times
    print("----The schema of the dataset:")
    carsDS.printSchema()

    var catList = loadCategoryNames(filename, catpos)
    print("----The list of categories:")
    println(catList)

    val stats = carsDS.groupBy(category_name).agg(mean(mean_column), variance(mean_column))
    println("Step 3. The mean and variance are calculated for each category:")
    stats.show()

    val sampleCache = carsDS.sample(false, percentage).cache()
    val the_sample = sampleCache.groupBy(category_name).agg(mean(mean_column), variance(mean_column)).cache()
    println("Step 4. The sample created for bootstrapping without replacement. Percentage: " + percentage)
    the_sample.show()

    //for each category, repeat the bootstrapping for #repeats times
    println("Step 5. Do the bootstrapping " + repeats + " times for each category")
    for (cat <- catList) {
      printme("Step 5a. Computing the mean and variance for the category " + cat)
      for (x <- 1 to repeats) {
        var temp = sampleCache.sample(true, 1).filter(col(category_name).equalTo(cat))
          .groupBy(category_name)
          .agg(mean(mean_column), variance(mean_column)).cache()
        printme(temp.show())
        for (r: Row <- temp.collect()) {
          val rcat = r(0).asInstanceOf[Double].toString()
          val rmean = r(1).asInstanceOf[Double]
          val rvar = r(2).asInstanceOf[Double]
          val stat = Stat(rcat, Pair(rmean, rvar))
          val category = stat.category
          val current = stat.pair
          var updated = Pair(current.mean, current.variance)

          var prev = catMap.getOrElse(category, null)
          if (prev != null) {
            updated = Pair(prev.mean + current.mean, prev.variance + current.variance)
          }
          printme(updated)
          catMap += (category -> updated)
        }
      }
      printme("Step 5c. Added the values:")
      printme(catMap)
    }
    println("Step 6. The average of the re-sampled datas:")
    catMap.map(x => (x._1, Pair(x._2.mean / repeats, x._2.variance / repeats)))
    println(catMap)
    spark.stop()

  }
  case class Car(mpg: Double, cyl: Double)
  case class Stat(category: String, pair: Pair)
  case class Pair(mean: Double, variance: Double)

  def mapper(line: String): Car = {
    val fields = line.split(',')
    val c: Car = Car(fields(1).toDouble, fields(2).toInt)
    return c
  }

  def loadCategoryNames(filename: String, catpos: Int): List[String] = {
    var catNames: HashSet[String] = HashSet()
    val lines = Source.fromFile(filename).getLines()
    for (line <- lines) {
      var fields = line.split(',')
      if (fields.length > 1)
        catNames += fields(catpos).toString()
    }
    return catNames.toList
  }
  def printme(obj: Any) = {
    //if (dev_mode == true)
    println(obj)
  }
}