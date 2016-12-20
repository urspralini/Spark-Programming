package com.prabhu.spark.programming

import org.apache.spark.{SparkContext, SparkConf}

/**
  * <p>
  *
  * </p>
  *
  * @author Prabhu R Babu
  *         (12/20/16 9:52 AM)
  */
object SparkStandAloneApp101 {

  def main(args: Array[String]) {
    val logFile = "/Users/pbabu/spark-2.0.2-bin-hadoop2.7/README.md"
    val conf = new SparkConf().setMaster("local")
      .setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile,2).cache()
    val numAs = logData.filter(line => line.contains("a"))
    val numBs = logData.filter(line => line.contains("b"))
    println(s"Lines with a: ${numAs.count()}, Lines with b: ${numBs.count()}")
    sc.stop()
  }

}
