package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


class Sftp extends SparkBatchSource {
  
  def ftpReader(sparkSession: SparkSession): DataFrame = {
    val path = config.getString("path")
    val input = sparkSession.read
              .format("com.springml.spark.sftp")
              .option("host", config.getString("host"))
              .option("username", config.getString("user"))
              .option("password", config.getString("password"))
              .option("port", if (config.getValueByValueNonEmpty("port").isEmpty) "22" else config.getString("port"))
              .option("fileType", if (config.getValueByValueNonEmpty("fileType").isEmpty) "csv" else config.getString("fileType"))
              .option("delimiter", if (config.getValueByValueNonEmpty("delimiter").isEmpty) ";" else config.getString("delimiter"))
              .option("head", if (config.getValueByValueNonEmpty("head").isEmpty) "true" else config.getString("head"))
              .option("quote", "\"")
              .option("escape", "\\")
              .option("multiLine", if (config.getValueByValueNonEmpty("multiLine").isEmpty) "true" else config.getString("multiLine"))
              .option("inferSchema", "true")
              .load(path)
      input
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    ftpReader(env.getSparkSession)
  }

  /**
   * 校验必输项
   */
  override def checkConfig(): CheckResult = {
    val requiredOptions = List("host", "path", "user", "password");
    val nonExistsOptions = requiredOptions
            .map(optionName => (optionName, config.hasPath(optionName)))
            .filter { p =>
              val (optionName, exists) = p
              !exists
            }
    if (nonExistsOptions.isEmpty) {
      new CheckResult(true, "")
    } else {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
                .map { case (field, _) => "[" + field + "]" }
                .mkString(", ") + " as non-empty string"
      )
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {}
}
