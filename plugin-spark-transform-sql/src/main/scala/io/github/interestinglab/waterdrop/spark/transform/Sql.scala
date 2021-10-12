package io.github.interestinglab.waterdrop.spark.transform

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

class Sql extends BaseSparkTransform {

  /**
   * 2021/3/3 扩展该方法
   * 1. 扩展 transform 自动添加 result_table_name 和 自动匹配 $t 表名
   * 2. transform 增加增量条件, 实现增量同步数据
   * @param data
   * @param env
   * @param executorTableName
   * @return
   */
  override def process(data: Dataset[Row], env: SparkEnvironment, executorTableName: String): Dataset[Row] = {
    var sql = config.getString("sql")

    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    if(sql.contains("$$startDate")) {
      sql = sql.replace("$$startDate", "'"+dateFormat.format(cal.getTime())+"'")
    }
    if(sql.contains("$$endDate")){
      sql = sql.replace("$$endDate", "'"+dateFormat.format(new Date())+"'")
    }

    if(sql.contains("$t")){
      env.getSparkSession.sql(sql.replace("$t", executorTableName))
    } else {
      env.getSparkSession.sql(sql)
    }
  }

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    env.getSparkSession.sql(config.getString("sql"))
  }

  override def checkConfig(): CheckResult = {
    if (config.hasPath("sql")) {
      new CheckResult(true, "")
    } else {
      new CheckResult(false, "please specify [sql]")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {}
}