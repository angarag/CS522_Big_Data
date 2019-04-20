package com.mars.sakai

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ stddev_samp, stddev_pop }
import scala.collection.immutable._
import scala.io.Source
  import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
  import Foo.DEVELOPMENT

object Foo{
  val DEVELOPMENT=false
//../mtcars.csv
//cyl
//mpg
//2
//3
//0.25
//5
}
object SparkProject extends App {
  val dev_mode: Boolean = true;
  override def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Initializing arguments
    var filename = "../mtcars.csv";
    var category_name = "cyl"
    var mean_column = "mpg"
    var catpos = 2 //category position in the file
    var valpos = catpos-1//2nd column
    var percentage = .25
    var repeats = 5;
    var catMap: Map[String, Pair] = Map() //sample(0)(0) -> (sample(0)(1), sample(0)(2)))

    if(args.length==7){
      filename=args(0)
      category_name=args(1)
      mean_column=args(2)
      catpos=args(3).toInt
      valpos=args(4).toInt
      percentage=args(5).toDouble
      repeats=args(6).toInt
    }
    else {println("Running the default configuration")
    println("Please enter the parameters in the following format:")
    println("$file_url $category_name $mean_column_name $category_column_position $fraction_percentage $#repeats")
    println("ie: '../mtcars.csv' cyl mpg 2 0.25 1000")
    }
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
    val carsDS = mtcdata.map(x=> mapper(x,catpos,valpos)).toDS()
    .withColumnRenamed("category", category_name)
    .withColumnRenamed("value", mean_column)
    .cache() //since we will run the dataset more than one, we will use cache so that we don't have reconstruct it multiple times
    print("----The schema of the dataset:")
    carsDS.printSchema()

    var catList = loadCategoryNames(filename, catpos).filter(!_.contains(category_name))
    print("----The list of categories:")
    println(catList)

    val stats = carsDS
    .groupBy(category_name).agg(mean(mean_column), variance(mean_column))
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
        val catrecords=sampleCache.sample(true, 1)
        .filter(col(category_name).equalTo(cat))
        var temp = catrecords
          .groupBy(category_name)
          .agg(mean(mean_column), variance(mean_column))
//          val observations = catrecords.rdd.map(row=>Vectors.dense(row.mpg))
//          val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
        //println(summary.variance)//the purpose is to compare it with rvar
        for (r: Row <- temp.collect()) {
          val rcat = r(0).asInstanceOf[String]
          val rmean = r(1).asInstanceOf[Double]
          var rvar = r(2).asInstanceOf[Double]
          if(rvar.isNaN())
            rvar=0.0
          val stat = Stat(rcat, Pair(rmean, rvar))
          val current_category = stat.category
          val current_pair = stat.pair
          var updated = Pair(current_pair.mean, current_pair.variance)

          var prev = catMap.getOrElse(current_category, null)
          if (prev != null) {
            updated = Pair(prev.mean + current_pair.mean, prev.variance + current_pair.variance)
          }
          printme(updated+" vs current:"+current_pair)
          catMap += (current_category -> updated)
        }
      }
      printme("Step 5c. Added the values:")
      printme(catMap)
    }
    println("Step 6. The average of the re-sampled datas:")
    catMap=catMap.map(x => (x._1, Pair(x._2.mean / repeats, x._2.variance / repeats)))
    println(catMap)
    spark.stop()

  }
  case class Car(category: String,value: Double) //value-category
  case class Stat(category: String, pair: Pair)
  case class Pair(mean: Double, variance: Double)

  def mapper(line: String, catpos:Int, valpos:Int): Car = {
    val fields = line.split(',')
    val c:Car = Car(fields(catpos).toString(),fields(valpos).toDouble)
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
    if (DEVELOPMENT == true)
    println(obj)
  }
}