package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.{CheckResult, TypesafeConfigUtils}
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class Doris extends SparkBatchSource {
  override def getData(env: SparkEnvironment): Dataset[Row] = {
    // Spark 直连 Doris 集群的方式
    // jdbcReader(env.getSparkSession).load()

    // Spark 通过 MySQL JDBC连接到 Doris 集群
    jdbcReader(env.getSparkSession, config.getString("driver")).load()
  }

  override def checkConfig(): CheckResult = {
//    val requiredOptions = List("fenodes", "table", "user", "password");
    val requiredOptions = List("url", "table", "user", "password");

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

  def jdbcReader(sparkSession: SparkSession, driver: String): DataFrameReader = {
    // Spark 直连 Doris 集群的方式
    // val reader = sparkSession.read
    //   .format("doris")
    //   .option("doris.table.identifier", config.getString("table"))  // "example_db.table3"
    //   .option("doris.fenodes", config.getString("fenodes")) // "10.82.97.36:8030"
    //   .option("user", config.getString("user"))
    //   .option("password", config.getString("password"))
    //
    // Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "doris.", false)) match {
    //
    //   case Success(options) => {
    //     val optionMap = options
    //       .entrySet()
    //       .foldRight(Map[String, String]())((entry, m) => {
    //         m + (entry.getKey -> entry.getValue.unwrapped().toString)
    //       })
    //
    //     reader.options(optionMap)
    //   }
    //   case Failure(exception) => // do nothing
    // }
    // reader

    // Spark 通过 MySQL JDBC连接到 Doris 集群
    val reader = sparkSession.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", driver)

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false)) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader.options(optionMap)
      }
      case Failure(exception) => // do nothing
    }
    reader
  }
}
