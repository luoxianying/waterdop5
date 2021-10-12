package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{Dataset, Row}

/**
 * create by kino on 2021/2/19
 */
class Kafka extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val conf = env.getSparkSession.sparkContext.broadcast(
      Map[String, Object](
        "bootstrap.servers" -> config.getString("broker"),
        "key.serializer" -> classOf[StringSerializer].getName,
        "value.serializer" -> classOf[StringSerializer].getName))
    
    data.toJSON.foreachPartition { it =>
      val sink: KafkaSink[String, Object] = KafkaSink[String, Object](conf.value)
      it.foreach(v => sink.send(config.getString("topic"), v))
      sink.producer.close()
    }
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("topic", "broker")
    val nonExistsOptions = requiredOptions.map(optionName => {
      (optionName, config.hasPath(optionName))
    }).filter{ p =>
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