package com.github.alinski.spark.zoo

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

import scala.io.Source
import scala.util.{Failure, Success, Try}

object SchemaUtils extends LazyLogging {

  case class SchemaValidationError(msg: String) extends Exception

  /** Reads a spark schema in json format into a {StructType}
    */
  def schemaFromFile(path: String): StructType = {
    Try(Source.fromFile(path).getLines.mkString) match {
      case Success(s: String)    => DataType.fromJson(s).asInstanceOf[StructType]
      case Failure(e: Exception) => throw e
    }
  }

  /** Returns true if dataframe has all columns present, false otherwise and logs a warning with
    * missing cols.
    */
  def hasColumns(df: DataFrame, cols: Seq[String]): Boolean = {
    val diff = cols.toSet.diff(df.schema.map(_.name).toSet).toList
    diff match {
      case x :: xs =>
        logger.warn(s"${diff.mkString(", ")} not present in the dataframe")
        false
      case _ => true
    }
  }

  /** Returns a [DataFrame] containing all fields in the schema. If fields don't exist, they will be
    * created will null values and the correct type.
    */
  def enforceSchema(df: DataFrame, schema: StructType): DataFrame = {
    val sourceSchema = df.schema
    val fieldsDiff = schema.map(_.name) filterNot sourceSchema.map(_.name).toSet
    if (fieldsDiff.nonEmpty) {
      val fieldsToAdd = schema.filter(fieldsDiff contains _.name)
      val newDf: DataFrame = fieldsToAdd.foldLeft(df) { (df, field) =>
        df.withColumn(field.name, lit(null).cast(field.dataType))
      }
      newDf.selectExpr(schema.map(_.name): _*)
    } else {
      if ((sourceSchema.map(_.name).toSet diff schema.map(_.name).toSet).nonEmpty) {
        df.selectExpr(schema.map(_.name): _*)
      } else {
        df
      }
    }
  }

  def tryApplyJSONSchema(
      df: DataFrame,
      field: String,
      schema: Option[StructType] = None
  ): DataFrame = {
    schema match {
      case Some(sch: StructType) =>
        df.withColumn(field, from_json(col(field).cast(StringType), sch))
      case _ => df
    }
  }

  /** Tries to parse the key and value fields into a struct with the schema of [[valueSchema]] and
    * [[keySchema]]. Must provide at least one of [[keySchema]] or [[valueSchema]].
    */
  def applyKafkaSchema(
      df: DataFrame,
      keySchema: Option[StructType] = None,
      valueSchema: Option[StructType] = None
  ): DataFrame = {
    assert(keySchema.isDefined || valueSchema.isDefined)
    assert(Set("key", "value").diff(df.schema.map(_.name).toSet).isEmpty)
    df
      .transform(tryApplyJSONSchema(_, "key", keySchema))
      .transform(tryApplyJSONSchema(_, "value", valueSchema))
  }
}
