package com.github.alinski.spark.zoo;

import com.github.alinski.spark.zoo.DataFrameHelpers._
import com.github.alinski.spark.zoo.SchemaUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class SchemaValidationSpec extends AnyFlatSpec with Matchers with TestUtils {

  private val df = spark.read
    .format("csv")
    .option("header", "true")
    .load(resourcePath("/samples/prices"))

  "Schema" should "be read from a json file" in {
    schemaFromFile(resourcePath("/schemas/prices.json"))
      .isInstanceOf[StructType] shouldBe true
  }

  "Schema validation" should "be ok" in {
    df.hasColumns(List("symbol", "close", "date")) shouldBe true
    df.hasColumns(List("symbol", "date", "foo", "bar")) shouldBe false
  }

  "Schema enforcement" should "add missing columns" in {
    val toDrop = List("high", "low", "volume")
    val dfDrop = df.drop(toDrop: _*)
    dfDrop.hasColumns(toDrop) shouldBe false
    val schema = schemaFromFile(resourcePath("/schemas/prices.json"))
    val dfNew = df.withColumn("foo", lit("barr")).enforceSchema(schema)
    dfNew.hasColumns(toDrop) shouldBe true
    dfNew.schema.map(_.name).toList.contains("foo") shouldBe false
  }

  "JSON schema application" should "work" in {
    val rawSchema = StructType(
      Seq(
        StructField("key", StringType, nullable = true),
        StructField("value", StringType, nullable = true)
      )
    )
    val parsedSchema = schemaFromFile(resourcePath("/schemas/publications.json"))
    val valueSchema = parsedSchema.filter(_.name == "value").head.dataType.asInstanceOf[StructType]
    val jsValue = """
    |{
    |    "author": {
    |      "lastname": "Doe",
    |      "firstname": "Jane"
    |    },
    |    "editor": {
    |      "lastname": "Smith",
    |      "firstname": "Jane"
    |    },
    |    "title": "The Ultimate Database Study Guide",
    |    "category": ["Non-Fiction", "Technology"]
    |}""".stripMargin.trim
    val rows = spark.sparkContext.parallelize(Seq(Row("123-456-22", jsValue)))
    val df = spark.createDataFrame(rows, rawSchema)
    val dfNew = SchemaUtils.tryApplyJSONSchema(df, "value", Some(valueSchema))
    dfNew.schema shouldEqual parsedSchema
    val authors = dfNew
      .selectExpr("value.author.firstname", "value.editor.lastname")
      .collect()
      .map(row =>
        (
          row.getAs[String]("firstname"),
          row.getAs[String]("lastname")
        )
      )
      .head
    authors shouldEqual ("Jane", "Smith")
  }

  "kafka schema" should "be applied" in {
    val df = spark.read.json(resourcePath("/samples/publications"))
    val parsedSchema = schemaFromFile(resourcePath("/schemas/publications.json"))
    val valueSchema =
      StructType(parsedSchema.filter(_.name == "value").head.dataType.asInstanceOf[StructType])
//    val dfNew = SchemaUtils.applyKafkaSchema(df, valueSchema = Some(valueSchema))
//    dfNew.show(false)
//    dfNew.where("value.author.firstname == 'Jane'").count shouldEqual 1
    Try(SchemaUtils.applyKafkaSchema(df)).isInstanceOf[Try[AssertionError]] shouldBe true
    val dfRenamed = df.withColumnRenamed("key", "foo")
    Try(SchemaUtils.applyKafkaSchema(dfRenamed)).isInstanceOf[Try[AssertionError]] shouldBe true
  }
}
