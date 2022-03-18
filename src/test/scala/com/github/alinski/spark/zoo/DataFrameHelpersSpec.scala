package com.github.alinski.spark.zoo

import com.github.alinski.spark.zoo.DataFrameHelpers._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataFrameHelpersSpec extends AnyFlatSpec with Matchers with TestUtils {

  private val df = spark.read
    .format("csv")
    .option("header", "true")
    .load(resourcePath("/samples/prices"))

  "Partition information" should "be ok in" in {
    val path = resourcePath("/samples/prices")
    df.getPath shouldEqual Some(s"file://$path")
    df.getPartitionFileCounts.keys.size shouldEqual 4
  }

//  "Repartitioning with an upper limit of records" should "work" in {
//    val initialFiles = df.rdd.partitions.length
//    val newDf = df.repartition(Seq("symbol"), 30)
//    newDf.show()
//    println(s"Initial: $initialFiles; now:  ${newDf.rdd.partitions.length}")
//  }

  "Minimum and maximum values" should "work" in {
    val minMaxValsGroups = df.getMinMax(Seq("symbol"), Seq("close"))
    minMaxValsGroups.count shouldEqual 4
    minMaxValsGroups.hasColumns(List("close_min", "close_max")) shouldBe true
    val minMaxVals = df.getMinMax(Seq("close"))
    minMaxVals.count shouldEqual 1
    minMaxValsGroups.hasColumns(List("close_min", "close_max")) shouldBe true
  }

  "Appending minimum and maximum values" should "work" in {
    val dfMinMaxGroups = df.appendMinMax(Seq("symbol"), Seq("close"))
    dfMinMaxGroups.hasColumns(Seq("close_min", "close_max")) shouldBe true
    dfMinMaxGroups.hasColumns(df.schema.map(_.name)) shouldBe true
    val dfMinMax = df.appendMinMax(Seq("close"))
    dfMinMax.hasColumns(Seq("close_min", "close_max")) shouldBe true
    dfMinMax.hasColumns(df.schema.map(_.name)) shouldBe true
  }

}
