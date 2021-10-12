package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters.seqAsJavaListConverter
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.sql.functions.col


/**
 * Created by kino on 2021/1/25.
 */
class Kudu extends SparkBatchSink {
  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    // 建表
    val kuduContext = createKuduTable(data, env, config.getString("table"))

    // 删表
    //deleteKuduTable(env, config.getString("table"))

    //写数据
    kuduContext.upsertRows(
      ds_change_type(data),
      config.getString("table"),
      new KuduWriteOptions(false, true))
  }

  /**
   * 初始化 KuduContext 对象
   * @param env
   * @return
   */
  def initKuduContext(env: SparkEnvironment) = {
    val kuduContext = new KuduContext(kuduMaster =
      if (!config.getString("kuduMaster").contains(":"))  config.getString("kuduMaster") + ":7051"
      else config.getString("kuduMaster"), env.getSparkSession.sparkContext)
    kuduContext
  }

  /**
   * 创建 Kudu 表
   * @param data
   * @param env
   * @param tableName
   * @return
   */
  def createKuduTable(data: Dataset[Row], env: SparkEnvironment, tableName: String) = {
    val kuduContext = initKuduContext(env)

    //判断表是否存在
    if(!kuduContext.tableExists(tableName)){
      val primary_key = Seq(config.getValueByValueNonEmpty("key")).map(_.toLowerCase)
      //建表
      val structType = schema_change(data.schema, primary_key)
      kuduContext.createTable(
        tableName,
        structType,
        primary_key,
        new CreateTableOptions()
          .setNumReplicas(1)
          .addHashPartitions(primary_key.asJava, 3)
      )
    }
    kuduContext
  }

  /**
   * 删除 Kudu 表
   * @param env
   * @param tableName
   */
  def deleteKuduTable(env: SparkEnvironment, tableName: String): Unit = {
    val kuduContext = initKuduContext(env)
    kuduContext.deleteTable(tableName)
  }

  /**
   * 将 Spark schema 转为kudu可以建表的schema
   *   1. 主键不能为空
   *   2. 把kudu不支持的数据类型转为String
   *   3. 所有列名转为小写字母
   * @param sch
   * @param pk
   * @return
   */
  def schema_change(sch: StructType, pk: Seq[String]) = {
    var arr = Array[StructField]()
    sch.foreach {
      case StructField(name: String, dataType: DataType, nullable: Boolean, metadata: Metadata) =>
        val name_ = name.toLowerCase
        val dataType_ = dataType match {
          case DateType => StringType
          case t: DecimalType => StringType
          case _ => dataType
        }
        var nullable_ = nullable
        pk.foreach(k => if (k.toLowerCase == name_) nullable_ = false)
        arr = arr :+ StructField(name_, dataType_, nullable_, metadata)
    }
    StructType(arr)
  }

  /**
   * 将 DataFrame中 kudu不支持的列类型转为String类型
   */
  def ds_change_type(ds: Dataset[Row]) = {
    val new_sch = schema_change(ds.schema, Seq())
    var d = ds
    new_sch.foreach {
      case StructField(name: String, dataType: DataType, _, _) =>
        d = d.withColumn(name, col(name).cast(dataType))
    }
    d
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("kuduMaster", "table", "key")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, !config.getValueByValueNonEmpty(optionName)
      .equals(""))).filter { p =>val (optionName, exists) = p
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
