package com.github.alinski.spark.zoo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait TestUtils {

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .config(new SparkConf().setMaster("local[*]"))
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  lazy val resourcePath: String => String = (path: String) => getClass.getResource(path).getPath

}
