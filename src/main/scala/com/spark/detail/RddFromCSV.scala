package com.spark.detail

import org.apache.spark.sql.SparkSession

object RddFromCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Rdd to CSV")
      .master("local[*]")
      .getOrCreate()
    val rddFromCSV = spark.sparkContext.textFile("/home/cheluvesha/Downloads/Wrong.csv")
    // separating delimiter using map
    val rdd = rddFromCSV.map(f => {
    f.split(",")})
    //stores 1st record
    val head = rdd.first()
    // removing header
    val withOutHeadRdd = rdd.filter(line => !(line sameElements head))
    withOutHeadRdd.take(10)
      .foreach(f => println(f(0)+" "+f(1)))
  }
}
