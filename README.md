# spark-zoo
Collection of Spark utilities to solve recurrent problems

<details open>
  <summary><b>Table of contents</b></summary>

---
- [Usage](#Usage)
- [Installation](#instlation)
- [Functionality](#functionality)
  - [DataFrame](#dataframe-methods)
  - [DeltaTable](#deltatable-methods)
  - [Helpers](#general-helpers)
- [Supported spark versions](#supported-spark-versions)
---

</details>

## **Instlation**
Currently you have to build it from source using sbt. Should be soon available on a public repository.
```bash
sbt clean assembly
```

## **Usage**
- For `DataFrame` functions, import the following, which will include more methods to the API.
```scala
import com.github.alinski.spark.zoo.DataFrameHelpers._
```
- For `DeltaTable`, accordingly:
```scala
import com.github.alinski.spark.zoo.DeltaTableHelpers._
```

## **Functionality**

- ### `DataFrame` methods
  - .getMinMax(cols: Seq[String]): DataFrame 
  - .getMinMax(groups: Seq[String], cols: Seq[String]): DataFrame
  - .appendMinMax(cols: Seq[String]): DataFrame
  - .appendMinMax(groups: Seq[String], cols: Seq[String]): DataFrame
  - .hasColumns(cols: Seq[String]): Boolean
  - .enforceSchema(schema: StructType): DataFrame
  - .getPath: Option[String]
  - .hasPartitions: Boolean
  - .getPartitionFileCounts: Map[String, Int]
  - .applyKafkaSchema(keySchema: Option[StructType] = None, valueSchema: Option[StructType] = None): DataFrame
  - .repartition(columns: Seq[String], maxPartitionRecords: Int): DataFrame
  - .addMetaFields: DataFrame

- ### `DeltaTable` methods
  - .upsert(updates: DataFrame, predicate: String, ...): Unit
  - .compact(maxFiles: Long, optimalFiles: Option[Long]): Unit 
  - .generateManifest(): Unit
  - .getOrCreate(path: String, updates, partitions, mode): DeltaTable
  - .generateInsertExpr
  - .generateUpdateExpr(updates, ignoreNulls = false, addMetaFields = false, ignoreIfSet = false)


- ### General helpers
  - .readSchemaFromFile(path: String): StructType
  
## **Supported Spark versions**
- 2.4.x
- 3.0.x
- 3.1.x