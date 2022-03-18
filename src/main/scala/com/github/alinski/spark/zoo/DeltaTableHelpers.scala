package com.github.alinski.spark.zoo

import com.github.alinski.spark.zoo.DataFrameHelpers._
import com.github.alinski.spark.zoo.SchemaUtils.SchemaValidationError
import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}

import scala.util.{Success, Try}

object DeltaTableHelpers extends LazyLogging {

  case class DeltaWriteError(msg: String) extends Exception
  private val metaCols =
    Seq("__first_update", "__last_update", "__count_updates")

  implicit class DeltaTableOps(dt: DeltaTable) {

    /** Updates the existing records using [[updateExpr]] and inserts new records using
      * [[insertExpr]]
      * @param updates:
      *   a [[DataFrame]] where the columns are at least a subset of the [[DeltaTable]]
      * @param predicate:
      *   a SQL predicate using aliases 'tbl' and 'upd', e.g.: "upd.id = tbl.id AND upd.year !=
      *   tbl.year"
      * @param updateExpr:
      *   only specify it for very custom logic, otherwise relies on [[generateUpdateExpr]]
      * @param insertExpr:
      *   only specify it for very custom logic, otherwise relies on [[generateInsertExpr]]
      * @param ignoreNulls:
      *   if true, null values in the updates will not overwrite the existing record in the table.
      * @param addMetaFields:
      *   see: [[addMetaFields]]
      * @param ignoreIfSet:
      *   those fields will never be updated by newer versions. useful for storing the first value.
      */
    def upsert(
        updates: DataFrame,
        predicate: String,
        updateExpr: Option[Map[String, String]] = None,
        insertExpr: Option[Map[String, String]] = None,
        ignoreNulls: Boolean = false,
        metaFields: Boolean = false,
        ignoreIfSet: List[String] = List()
    ): Unit = {
      val update = updateExpr.getOrElse(
        generateUpdateExpr(updates, ignoreNulls, metaFields, ignoreIfSet)
      )
      val upd =
        if (metaFields && !updates.hasColumns(metaCols)) updates.addMetaFields
        else updates
      val insert = insertExpr.getOrElse(generateInsertExpr(updates))
      dt.as("tbl")
        .merge(upd.as("upd"), predicate)
        .whenMatched()
        .updateExpr(update)
        .whenNotMatched()
        .insertExpr(insert)
        .execute()
    }

    /** Rewrites the delta table with a desired number of files, only if the existing number is
      * larger. for partitioned data, [[maxFiles]] and [[optimalFiles]] theis represents the max
      * files per partion.
      * @param optimalFiles:
      *   if not provided, defaults to "spark.sql.shuffle.partitions"
      */
    def compact(maxFiles: Long, optimalFiles: Option[Long]): Unit = {
      val df = dt.toDF
      val path = df.getPath
      if (path.isEmpty) {
        throw new Exception("Couldn't get the DeltaTable path.")
      }
      val optFiles = optimalFiles.getOrElse(
        df.sparkSession.conf.get("spark.sql.shuffle.partitions").toLong
      )
      if (dt.toDF.hasPartitions) {
        val oversizePartitions: Map[String, Int] =
          df.getPartitionFileCounts.filter(_._2 > maxFiles)
        oversizePartitions.size match {
          case 0 => ()
          case _ =>
            oversizePartitions.foreach { case (partition, count) =>
              val filter = generatePartitionFilter(partition)
              val newDf = df.where(filter).repartition(optFiles.toInt)
              logger.warn(
                s"Will rewrite data for partition matching: '${filter}' into ${optFiles} partitions"
              )
              val opts = Map("dataChange" -> "false", "replaceWhere" -> filter)
              writeAsDelta(newDf, path.get, opts)
//            newDf.write.format("delta")
//              .options(Map(
//                "dataChange" -> "false",
//                "replaceWhere" -> filter
//              ))
//              .mode("overwrite")
//              .save(path.get)
            }
        }
      } else {
        val dtFiles = df.inputFiles.length
        if (dtFiles >= maxFiles) {
          logger.warn(
            s"Delta table exceeded file threshold: $dtFiles >= $optFiles." + " Will rewrite the data."
          )
          val newDf = df.repartition(optFiles.toInt)
          writeAsDelta(newDf, path.get, Map("dataChange" -> "false"))
        }
        logger.info(
          s"Delta table does not exceed the maximum number of files: $dtFiles < $optFiles."
        )
      }
    }

    /** Creates a manifest file, required for systems like Presto or Athena
      */
    def generateManifest(): Unit = dt.generate("symlink_format_manifest")
  }

  /** Returns a [[DeltaTable]] from a path if exists, otherwise tries to create one from a
    * dataframe.
    * @param updates:
    *   [[DataFrame]] which will be used to create the [[DeltaTable]] if it does not exist
    */
  def getOrCreate(
      path: String,
      updates: Option[DataFrame] = None,
      partitions: Seq[String] = Seq(),
      mode: String = "overwrite"
  ): DeltaTable = {
    Try(DeltaTable.forPath(path)) match {
      case Success(table: DeltaTable) =>
        logger.debug(s"Found delta table at path: '$path'.")
        table
      case _ =>
        if (updates.isEmpty) {
          val msg = "No delta table at path: '$path' and no updates provided"
          logger.error(msg)
          throw DeltaWriteError(msg)
        }
        writeAsDelta(updates.get, path, Map(), partitions, mode)
        DeltaTable.forPath(path)
    }
  }

  /** Creates a Map(source -> sink) fields to be used by delta merge
    */
  def generateInsertExpr(updates: DataFrame): Map[String, String] = {
    updates.schema.map(_.name).map(field => field -> s"upd.$field").toMap
  }

  /** Returns a Map[col source => col sink] to be used by delta merge.
    * @param ignoreNulls:
    *   if true, null values in the updates will not overwrite the existing record in the table.
    * @param addMetaFields:
    *   see: [[addMetaFields]]
    * @param ignoreIfSet:
    *   those fields will never be updated by newer versions. useful for storing the first value.
    */
  def generateUpdateExpr(
      updates: DataFrame,
      ignoreNulls: Boolean = false,
      addMetaFields: Boolean = false,
      ignoreIfSet: List[String] = List()
  ): Map[String, String] = {
    val fields = updates.toDF.schema
      .map(_.name)
      .filterNot(Seq("__first_update", "__count_updates") contains _)
    val mappedFields = if (ignoreNulls) {
      fields.map(f => f -> s"CASE WHEN upd.$f IS NOT NULL THEN upd.$f ELSE tbl.$f END")
    } else {
      fields.map(f => f -> s"upd.$f")
    }.toMap
    val metaFields = if (addMetaFields) {
      Map(
        "__first_update" -> "COALESCE(tbl.__first_update, upd.__first_update)",
        "__count_updates" -> "COALESCE(tbl.__count_updates, 1) + COALESCE(upd.__count_updates, 1)"
      )
    } else {
      Map()
    }
    if (ignoreIfSet.isEmpty) {
      return (mappedFields ++ metaFields).toMap
    }
    (mappedFields ++ metaFields).map { case (k, v) =>
      if (ignoreIfSet contains k) {
        k -> s"CASE WHEN tbl.$k IS NOT NULL THEN tbl.$k ELSE upd.$k END"
      } else {
        k -> v
      }
    }.toMap
  }

  private def generatePartitionFilter(partition: String): String = {
    val partitions = partition.split("/").toList
    partitions
      .map(p => {
        val splits = p.split("=")
        s"${splits.head} IN ('${splits.tail.mkString(", ")}')"
      })
      .mkString(" AND ")
  }

  private def writeAsDelta(
      df: DataFrame,
      path: String,
      options: Map[String, String] = Map(),
      partitions: Seq[String] = Seq(),
      mode: String = "overwrite"
  ): Unit = {
    val dfWriter = df.write.options(options).mode(mode).format("delta")
    val writer = if (partitions.nonEmpty) {
      if (!df.hasColumns(partitions))
        throw SchemaValidationError(
          s"Some of ${partitions.mkString(", ")} not present in dataframe."
        )
      dfWriter.partitionBy(partitions: _*)
    } else {
      dfWriter
    }
    writer.save(path)
  }
}
