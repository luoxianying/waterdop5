package io.github.interestinglab.waterdrop.spark.batch

import java.util.{Date, List => JList}
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.{CheckResult, ConfigRuntimeException}
import io.github.interestinglab.waterdrop.env.Execution
import io.github.interestinglab.waterdrop.spark.dialect.MyDialect
import io.github.interestinglab.waterdrop.spark.{BaseSparkSink, BaseSparkSource, BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import java.text.SimpleDateFormat
import java.time.LocalDate
import scala.collection.JavaConversions._

class SparkBatchExecution(environment: SparkEnvironment)
  extends Execution[SparkBatchSource, BaseSparkTransform, SparkBatchSink] {

  private var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(prepareEnv: Void): Unit = {}

  override def start(sources: JList[SparkBatchSource],
                     transforms: JList[BaseSparkTransform],
                     sinks: JList[SparkBatchSink]): Unit = {

    sources.foreach(s => {
      SparkBatchExecution.registerInputTempView(s.asInstanceOf[BaseSparkSource[Dataset[Row]]], environment)
    })

    if (!sources.isEmpty) {
      var ds: Dataset[Row] = sources.get(0).getData(environment)
      for (tf <- transforms) {

        //        if (ds.take(1).length > 0) {
        ds = SparkBatchExecution.transformProcess(environment, tf, ds)
        //SparkBatchExecution.registerTransformTempView(tf, ds)
        //        }
      }

      // environment.getSparkSession.sql("select * from tab1").show(10, false)
      // if (ds.take(1).length > 0) {
      sinks.foreach(sink => {
        SparkBatchExecution.sinkProcess(environment, sink, ds)
      })
      // }
    }
  }

}


object SparkBatchExecution {

  private[waterdrop] val sourceTableName = "source_table_name"
  private[waterdrop] val resultTableName = "result_table_name"

  private[waterdrop] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
  }

  private[waterdrop] def registerInputTempView(source: BaseSparkSource[Dataset[Row]], environment: SparkEnvironment): Unit = {
    val conf = source.getConfig
    conf.hasPath(SparkBatchExecution.resultTableName) match {
      case true => {
        if (conf.getValueByValueNonEmpty("url").contains("jdbc:mysql")) {
          MyDialect.useMyJdbcDialect(conf.getString("url"))
        }
        val tableName = conf.getString(SparkBatchExecution.resultTableName)
        registerTempView(tableName,  source.getData(environment))
      }
      case false => {
        throw new ConfigRuntimeException(
          "Plugin[" + source.getClass.getName + "] must be registered as dataset/table, please set \"result_table_name\" config")
      }
    }
  }

  private[waterdrop] def transformProcess(environment: SparkEnvironment, transform: BaseSparkTransform, ds: Dataset[Row]): Dataset[Row] = {
    val config: Config = transform.getConfig()
    config.hasPath(SparkBatchExecution.sourceTableName) match {
      case true => {
        // 源码原始方法
        // val sourceTableName = config.getString(SparkBatchExecution.sourceTableName)
        // environment.getSparkSession.read.table(sourceTableName)

        // 修改后 -> 扩展Transform对多个数据源进行转换
        var resultDf = environment.getSparkSession.emptyDataFrame
        var sourceTableNameArrays = Array[String]()

        if(config.hasPath(SparkBatchExecution.sourceTableName)){
          sourceTableNameArrays = config.getString(SparkBatchExecution.sourceTableName).split(",")
          sourceTableNameArrays.foreach(x => {
            resultDf = transform.process(ds, environment, x)
            environment.getSparkSession.catalog.dropTempView(x)
            registerTempView(x, resultDf)
          })
        } else {
          environment.getSparkSession.catalog.dropTempView(config.getString(SparkBatchExecution.resultTableName))
          registerTempView(config.getString(SparkBatchExecution.resultTableName), resultDf)
        }
        resultDf
      }
      case false => {
        // 没有 source_table_name, 将上一个插件的结果集进行 Transform 操作
        val value: Dataset[Row] = transform.process(ds, environment)
        value
      }
    }
  }

  private[waterdrop] def registerTransformTempView(plugin: BaseSparkTransform, ds: Dataset[Row]): Unit = {
    val config = plugin.getConfig()
    if (config.hasPath(SparkBatchExecution.resultTableName)) {
      val tableName = config.getString(SparkBatchExecution.resultTableName)
      val sourceTableName = config.getString(SparkBatchExecution.sourceTableName)
      registerTempView(sourceTableName, ds)
    }
  }

  private[waterdrop] def sinkProcess(environment: SparkEnvironment, sink: BaseSparkSink[_], ds: Dataset[Row]): Unit = {
    val config = sink.getConfig()
    val fromDs: Dataset[Row] = config.hasPath(SparkBatchExecution.sourceTableName) match {
      case true => {
        // 源码原始方法
        // val sourceTableName = config.getString(SparkBatchExecution.sourceTableName)
        // environment.getSparkSession.read.table(sourceTableName)

        // 修改后 -> 增加sink选择多个数据源
        val sourceTableName = config.getString(SparkBatchExecution.sourceTableName)
        val tableNameArrays = sourceTableName.split(",")
        var result_dataset: Dataset[Row] = environment.getSparkSession.read.table(tableNameArrays(0))
        for (i <- 1 until tableNameArrays.size){
          result_dataset = result_dataset.union(environment.getSparkSession.read.table(tableNameArrays(i)))
        }
        //        import org.apache.spark.sql.functions._
        result_dataset
        //          .withColumn("etl_create_time", lit(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())))
        //          .withColumn("etl_update_time", lit(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())))
      }
      case false => ds
    }
    // 应雷哥要求, 增加每个插件单独执行时间的日志记录(新大正POC)
    val startTime = System.currentTimeMillis()
    sink.output(fromDs, environment)
    val endTime = System.currentTimeMillis()
    System.out.println(s"************ The current plugin execution time is: "+ (endTime - startTime) + " ms")
  }
}
