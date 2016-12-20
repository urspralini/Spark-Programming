package com.prabhu.spark.programming

import org.apache.spark.{SparkContext, SparkConf}

/**
  * <p>
  *
  * </p>
  *
  * @author Prabhu R Babu
  *         (12/20/16 10:10 AM)
  */
object WordCountApp {

  def main(args: Array[String]) {
    //Create spark context
    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)
    //Load input data
    val inputFile = "/Users/pbabu/spark-2.0.2-bin-hadoop2.7/README.md"
    val outputFile = "/tmp/wordCountApp"
    val lines = sc.textFile(inputFile)

    //Split the lines into words and call count
    val words = lines.flatMap(line => line.split(" "))
    println(s"Total number of words:${words.count()}")

    //Frequency Count
    val counts = words.map(word => (word,1)).reduceByKey{case (x,y) => x+y}

    //save the counts in the output file
    counts.saveAsTextFile(outputFile)

    //close the spark appln
    sc.stop()
  }
}
