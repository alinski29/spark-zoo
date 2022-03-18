package com.github.alinski.spark.zoo

import com.github.alinski.spark.zoo.DataFrameHelpers._
import com.github.alinski.spark.zoo.DeltaTableHelpers._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.reflect.io.Directory

class DeltaTableHelpersSpec extends AnyFlatSpec with Matchers with TestUtils {

  private def cleanup(path: String): Unit = {
    val dir = new Directory(new File(path))
    if (dir.exists) dir.deleteRecursively()
  }

  private val df = spark.read
    .format("csv")
    .option("header", "true")
    .load(resourcePath("/samples/prices"))
    .withColumn("date", col("date").cast(DateType))
    .withColumn("month", date_format(col("date"), "yyyyMM"))
    .withColumn(
      "quarter",
      concat_ws("Q", year(col("date")).cast(StringType), quarter(col("date").cast(StringType)))
    )

  private def avgByQuarter(df: DataFrame): DataFrame = {
    df
      .groupBy("symbol", "quarter")
      .agg(avg(col("close")).as("close_avg"))
      .orderBy("symbol", "quarter")
      .addMetaFields
  }

  private val firstUpdate = avgByQuarter(df.where("month <= '202004'"))
  private val secondUpdate = avgByQuarter(df.where("month <= '202007'"))
  private val lastUpdate = avgByQuarter(df.where("month > '202008'"))
  private val metaCols = Seq("__first_update", "__last_update", "__count_updates")

  val path = "src/test/resources/samples/delta/prices"
  cleanup(path)

  "Delta table" should "be create from a dataframe" in {
//    val path = resourcePath("/samples/delta/prices")
    val dt = DeltaTableHelpers.getOrCreate(path, Some(firstUpdate), partitions = Seq("symbol"))
    dt.toDF.getPath.isDefined shouldBe true
    dt.toDF.count shouldEqual 8
  }

  "Delta table created a previous step" should "be read back" in {
    val dt = DeltaTableHelpers.getOrCreate(path, partitions = Seq("symbol"))
    dt.toDF.count shouldEqual 8
    dt.toDF.hasPartitions shouldBe true
  }

  "Adding meta fields" should "add new fields" in {
    secondUpdate.addMetaFields.hasColumns(metaCols) shouldBe true
  }

  "Generating insert expression" should "work" in {
    val insertExpr = DeltaTableHelpers.generateInsertExpr(secondUpdate)
    insertExpr.keys.toSet diff secondUpdate.schema.map(_.name).toSet shouldBe empty
  }

  "Generating update expression" should "work" in {
    val updateExpr = DeltaTableHelpers.generateUpdateExpr(secondUpdate)
    updateExpr.keys.toSet diff secondUpdate.schema.map(_.name).toSet shouldBe empty
  }

  "Upsert to a delta table" should "work" in {
    val dt = DeltaTable.forPath(path)
    dt.upsert(
      secondUpdate,
      "upd.symbol = tbl.symbol AND upd.quarter = tbl.quarter",
      metaFields = true
    )
    dt.toDF.count shouldEqual 12
    dt.toDF.hasColumns(metaCols) shouldEqual true
    dt.toDF.where("quarter IN ('2020Q3')").count shouldEqual 4
    dt.toDF.where("__count_updates > 1 AND __last_update > __first_update").count shouldEqual 8
  }

  "Upsert again to a delta table" should "also work" in {
    val dt = DeltaTable.forPath(path)
    dt.upsert(
      lastUpdate,
      "upd.symbol = tbl.symbol AND upd.quarter = tbl.quarter",
      metaFields = true
    )
    dt.toDF.count shouldEqual 16
    dt.toDF.where("quarter IN ('2020Q4')").count shouldEqual 4
    dt.toDF
      .where("quarter IN ('2020Q4')")
      .selectExpr("MAX(__count_updates)")
      .count shouldEqual 1
  }

  "Compact a delta table" should "work" in {
    val dt = DeltaTable.forPath(path)
    dt.compact(2, Some(1))
    val counts = dt.toDF.getPartitionFileCounts
    counts.values.sum shouldEqual counts.keys.size
  }

}
