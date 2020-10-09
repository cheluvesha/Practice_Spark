package com.spark.detail

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ListToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("List to DataFrame")
      .getOrCreate()
    // List to RDD and TO DataFrame
    val listData = List(("Shiva","1000"),("Vesha","4000"),("Bharath","3000"))
    val column = List("Name","ID")
    val rdd = spark.sparkContext.parallelize(listData)
    val df = spark.createDataFrame(rdd).toDF(column:_*)
    df.show();

    // using schema Struct type
    val schema = StructType(column
      .map(field => StructField("Name",StringType,nullable = true)))
    // rdd to row rdd convert RDD[T] to RDD[Row]
    val rowRdd = rdd.map(attribute => Row(attribute._1,attribute._2))
    val dff = spark.createDataFrame(rowRdd,schema)
    dff.take(1).foreach(println)

    // using seq

  }
}
