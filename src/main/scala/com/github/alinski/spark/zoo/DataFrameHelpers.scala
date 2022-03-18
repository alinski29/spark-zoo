package com.github.alinski.spark.zoo

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object DataFrameHelpers {

  implicit class DataFrameOps(df: DataFrame) {

    /** Returns a DataFrame with 1 row, and (2 * cols.length) columns, formatted as $col_min,
      * $col_max
      */
    def getMinMax(cols: Seq[String]): DataFrame = {
      val colsExpr = cols.flatMap(c =>
        Seq(
          min(col(c)).as(s"${c}_min"),
          max(col(c)).as(s"${c}_max")
        )
      )
      df.select(colsExpr: _*)
    }

    /** Returns a DataFrame of [$group, $col_min, $col_max]
      */
    def getMinMax(groups: Seq[String], cols: Seq[String]): DataFrame = {
      val colsExpr = cols.flatMap(c =>
        Seq(
          min(col(c)).as(s"${c}_min"),
          max(col(c)).as(s"${c}_max")
        )
      )
      df
        .groupBy(groups.map(col): _*)
        .agg(colsExpr.head, colsExpr.tail: _*)
    }

    /** Adds 2 * cols.length new columns to the dataframe, formatted as $col_min, $col_max
      */
    def appendMinMax(cols: Seq[String]): DataFrame = {
      val minMax = df.getMinMax(cols)
      df.crossJoin(minMax.drop(cols: _*))
    }

    /** Adds columns with the min/max values within each group, formatted as $col_min, $col_max
      */
    def appendMinMax(groups: Seq[String], cols: Seq[String]): DataFrame = {
      val minMax = df.getMinMax(groups, cols).drop(cols: _*)
      df.join(minMax, groups, "left")
    }

    def hasColumns(cols: Seq[String]): Boolean =
      SchemaUtils.hasColumns(df, cols)
    def enforceSchema(schema: StructType): DataFrame =
      SchemaUtils.enforceSchema(df, schema)

    lazy private val os = System.getProperty("os.name").toLowerCase
    lazy private val delim = if (os.startsWith("windows")) """\""" else "/"
    private lazy val partPattern = "([^{=]+)=([^,}]+)"

    def getPath: Option[String] = {
      df.inputFiles.toList match {
        case h :: t =>
          val splits = h.split(delim).toList
          val path = splits
            .dropRight(1)
            .filterNot(_.matches(partPattern))
            .mkString(delim)
          Some(path)
        case _ => None
      }
    }

    def hasPartitions: Boolean = {
      df.inputFiles.toList match {
        case h :: t => Seq(h, t.last).map(_.matches(partPattern)).reduce(_ || _)
        case _      => false
      }
    }

    def getPartitionFileCounts: Map[String, Int] = {
      val files = df.inputFiles.toList
      val partitions = files
        .map(path => {
          val splits = path.split("/").toList
          val partition = splits.tail
            .filter(_.matches("([^{=]+)=([^,}]+)"))
            .mkString("/")
          (partition, splits.last)
        })
        .filter(_._1.nonEmpty)
      if (partitions.isEmpty) {
        return Map()
      }
      partitions.groupBy(_._1).mapValues(_.length)
    }

    def applyKafkaSchema(
        keySchema: Option[StructType] = None,
        valueSchema: Option[StructType] = None
    ): DataFrame =
      SchemaUtils.applyKafkaSchema(df, keySchema, valueSchema)

    /** Will repartition the data by the specified columns, and each partition will have no max
      * number of {maxPartitionRecords}.
      */
    def repartition(
        columns: Seq[String],
        maxPartitionRecords: Int
    ): DataFrame = {
      val partitionCounts = df.groupBy(columns.map(col): _*).count()
      partitionCounts.show()
      df.join(partitionCounts, columns)
        .withColumn(
          "partitionKey",
          expr(s"cast((rand() * count / $maxPartitionRecords) AS INT)")
        )
        .repartition((columns :+ "partitionKey").map(col): _*)
        .drop(Seq("count", "partitionKey"): _*)
    }

    /** Adds 3 columns to store some useful metadata for keeping track of row updates.
      */
    def addMetaFields: DataFrame = {
      df
        .withColumn("__first_update", current_timestamp())
        .withColumn("__last_update", current_timestamp())
        .withColumn("__count_updates", lit(1))
    }
  }
}
