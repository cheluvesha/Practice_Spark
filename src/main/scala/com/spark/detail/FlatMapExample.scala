package com.spark.detail

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("FlatMap")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
    )

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    import spark.implicits._
    val dfFromMap = df.flatMap(value => value.getSeq[String](1)
      .map((value.getString(0),_,value.getString(2)))).toDF("Name","Language","state")
    dfFromMap.show()

  }
}
