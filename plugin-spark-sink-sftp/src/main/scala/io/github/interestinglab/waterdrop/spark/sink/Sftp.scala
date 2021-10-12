package io.github.interestinglab.waterdrop.spark.sink
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row}

class Sftp extends SparkBatchSink{
  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    config.getString("fileType") match {
      case "csv" | "json" | "txt" => {

        var opts = Map(
          "host" -> config.getString("host"),
          "username" -> config.getString("user"),
          "password" -> config.getString("password")
        )

        // 当写出模式是 csv时, 设置仅csv可设置的参数
        if (config.getString("fileType").equals("csv")) {
          opts += ("quote" -> "\"")   // 设置引号字符
          opts += ("escape" -> "\\")   // 设置转义字符
          opts += ("inferSchema" -> "true")   // 设置schema 推断模式
          opts += ("multiLine" -> (if (config.getString("multiLine").isEmpty) "true" else config.getString("multiLine")))  // 设置多行
          // 设置压缩解码器
          if (config.getValueByValueNonEmpty("codec").nonEmpty)
            opts += ("codec" -> "bzip2")
        }

        // 设置通用配置属性
        opts += (if (config.getValueByValueNonEmpty("port").nonEmpty) ("port" -> config.getString("port")) else ("port" -> "22"))
        opts += (if (config.getValueByValueNonEmpty("fileType").nonEmpty) ("fileType" -> config.getString("fileType")) else ("fileType" -> "csv"))
        opts += (if (config.getValueByValueNonEmpty("delimiter").nonEmpty) ("delimiter" -> config.getString("delimiter")) else ("delimiter" -> ","))
        opts += (if (config.getValueByValueNonEmpty("head").nonEmpty) ("head" -> config.getString("head")) else ("head" -> "true"))


        data.write
          .format("com.springml.spark.sftp")
          .options(opts)
          .save(config.getString("path"))
      }
      case _ => {
        println(s":::::::::::::${config.getString("fileType")} 是不支持的类型, 仅支持 csv/json/txt:::::::::::::::")
      }
    }
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("host", "user", "password", "path")
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
