package io.github.interestinglab.waterdrop.spark

import java.lang
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.env.RuntimeEnv
//import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

class SparkEnvironment extends RuntimeEnv {

  private var sparkSession: SparkSession = _

  private var streamingContext: StreamingContext = _

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(prepareEnv: lang.Boolean): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val sparkConf = createSparkConf()
    sparkSession = SparkSession
      .builder()
      .config(sparkConf)
//      .master("local[2]")
      .appName("jz-dmp-spark-"+config.getString("spark.app.name"))
      .enableHiveSupport()
      .getOrCreate()

    // Spark 读取 高可用NameNoe 的方式一
    val sc: SparkContext = sparkSession.sparkContext
    //    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    //    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    //    sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice1", "namenode25,namenode131")
    //    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode25", "bigdata001:8020")
    //    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode131", "bigdata002:8020")
    //    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    // Spark 读取 高可用NameNoe 的方式二
//    sc.hadoopConfiguration.addResource("core-site.xml")
//    sc.hadoopConfiguration.addResource("hdfs-site.xml")

//    sc.hadoopConfiguration.addResource(new Path(s"/etc/hive/conf/core-site.xml"))
//    sc.hadoopConfiguration.addResource(new Path(s"/etc/hive/conf/hdfs-site.xml"))
//    sc.hadoopConfiguration.addResource(new Path(s"/etc/hive/conf/hive-site.xml"))

    createStreamingContext
  }

  private def createSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    config
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }

  private def createStreamingContext: StreamingContext = {
    val conf = sparkSession.sparkContext.getConf
    val duration = conf.getLong("spark.stream.batchDuration", 5)
    if (streamingContext == null) {
      streamingContext =
        new StreamingContext(sparkSession.sparkContext, Seconds(duration))
    }
    streamingContext
  }

  def getStreamingContext: StreamingContext = {
    streamingContext
  }

  def getSparkSession: SparkSession = {
    sparkSession
  }

}
