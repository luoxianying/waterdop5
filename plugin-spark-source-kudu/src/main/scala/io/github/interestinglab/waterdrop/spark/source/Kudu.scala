package io.github.interestinglab.waterdrop.spark.source


import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.{SparkBatchSink, SparkBatchSource}
import org.apache.spark.sql.{Dataset, Row}

class Kudu extends SparkBatchSource {
  override def getData(env: SparkEnvironment): Dataset[Row] = {
    //    Map("kudu.master" -> "master:7051", "kudu.table" -> "impala::test_db.test_table")

    println("*******************************kudu的表名是:    impala::" + config.getString("table"))
    val opts = Map(
      "kudu.master" -> config.getString("kuduMaster"),
      "kudu.table" -> ("impala::"+config.getString("table"))
    )

    env.getSparkSession.read.format("org.apache.kudu.spark.kudu")
      .options(opts)
      .load()
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("kuduMaster", "table")
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

  override def prepare(prepareEnv: SparkEnvironment): Unit = {}
}
