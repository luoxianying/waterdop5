package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import scala.collection.mutable.ArrayBuffer

class Hive extends SparkBatchSink {
  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    if(config.getString("save_mode").toLowerCase.contains("overwrite")) {
//      data.write.mode(config.getString("save_mode")).option("truncate", "true").format("Hive").saveAsTable(config.getString("target_table"))
      data
        .write
        .format("Hive")
        .mode(config.getString("save_mode"))
        .option("truncate", "true")
        .insertInto(config.getString("target_table"))
    } else {
//      data.write.mode(config.getString("save_mode")).format("Hive").saveAsTable(config.getString("target_table"))
      data.write.mode(config.getString("save_mode")).format("Hive").insertInto(config.getString("target_table"))
    }
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("target_table", "save_mode")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.nonEmpty) {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    } else {
      new CheckResult(true, "")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {

  }
}
