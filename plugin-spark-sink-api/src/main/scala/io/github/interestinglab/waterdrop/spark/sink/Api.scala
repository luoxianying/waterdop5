package io.github.interestinglab.waterdrop.spark.sink

import com.alibaba.fastjson.serializer.SerializerFeature

import java.util
import com.alibaba.fastjson.{JSON, JSONObject}
import com.jz.api.com.jz.gateway.common.signtype.signutil.{MD5, RSAUtil, Sha256}
import io.github.interestinglab.waterdrop.common.config.{CheckResult, TypesafeConfigUtils}
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import io.github.interestinglab.waterdrop.spark.sink.Api.getTimestamp
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{DefaultHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
//import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Date
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}


/**
 * create by kino on 2021/2/26
 *
 * sql to api 插件包含两种接口的功能:
 *   1. 普通的 sql to api:
 *      - 业务逻辑:
 *        在 dmp 业务流程中选择 sql 任务(执行引擎:Spark 是前提), sql 任务中写 select 语句(sql to api 的数据源就是 select 查出来的结果),
 *        且需要在页面填 "源表字段" 和 "目标字段", 这两个字段需要做 as 转换, 且 "源表字段" 需要判定一定存于 select 查询的结果中, 如果不存在就抛异常结束,
 *        select 查询到多少条记录就发送多少次请求到 gateway, 每次gateway 都应该有对应的相应返回给到这里,
 *        TODO: 需要支持将log保存到 Kafka 和 HDFS
 *      - 发到gateway的json:
 *          {
 *              "apiKey":"s58vgxPacP2lWY79",
 *              "method":"request",
 *              "sign":"19F74799CA1DA5FB836C768A9970F438",
 *              "params":{
 *                  "authCode":"202100000552820124V0096aevhjE7s1",
 *                  "reqParams":{
 *                      "request_fileds":{
 *                          # 这里的值看 select 语句查询了多少个 column
 *                          "api_id":"QC002APIGOODS"
 *                      }
 *                  },
 *                  "isTest":false
 *              },
 *              "timestamp":"1622204141000"
 *          }
 *   2. 雀巢的 sql to api:
 *      - 业务逻辑:
 *        在 dmp 业务流程中选择 sql 任务(执行引擎:Spark 是前提), sql 任务中写 select 语句(sql to api 的数据源就是 select 查出来的结果),
 *        且需要在页面填 "源表字段" 和 "目标字段", 这两个字段需要做 as 转换, 且 "源表字段" 需要判定一定存于 select 查询的结果中, 如果不存在就抛异常结束,
 *        select 只会查询 data、batchId 两个字段(batchId 不重复), 将这两个字段封装到 request_fileds 中,
 *        select 查询会查到 >= 1 个批次的数据(>= 1 条记录), 每个批次的数据组装成一条 json 发送到 gateway,
 *        gateway 会返回请求消息回来, 如果状态是 200, 需要将这个 batchId 的记录 insert 到一张表中(暂时写死),
 *        TODO: 需要支持将log保存到 Kafka 和 HDFS
 *      - 雀巢的api插件发到gateway的json:
 *          {
 *              "apiKey":"s58vgxPacP2lWY79",
 *              "method":"request",
 *              "sign":"19F74799CA1DA5FB836C768A9970F438",
 *              "params":{
 *                  "authCode":"202100000552820124V0096aevhjE7s1",
 *                  "reqParams":{
 *                      "request_fileds":{
 *                          # 从数据库查询出来的 data 字段
 *                          "date":"QC002APIGOODS",
 *                          # 从数据库查询出来的 batchId 字段
 *                          "batchId":"1"
 *                      }
 *                  }
 *              },
 *              "timestamp":"1622204141000"
 *          }
 */
class Api extends SparkBatchSink {
  override def output(ds: Dataset[Row], env: SparkEnvironment): Unit = {
    getReceiveJson()
    val timestamp = getTimestamp()

    // 校验字段是否是查出来了
    checkSchema(ds.schema)

    // 获取 sign
    val sign = getSign(timestamp)

    // 将查出来的字段做别名转换准备 sink
    val select_columns = config.getString("mapping").replaceAll(":", " as ")

    // 将查询出来的结果用变量保存
    val data = getSqlAliasChange(ds, s"""select ${select_columns} from api""", env.getSparkSession)

    // 封装 JSON 对外层的参数: apiKey、method、sign(根据不同的 signType 选择不同的加密方式)、params(JSON Array)、timestamp
    val head_json = new util.HashMap[String, Object]()
    head_json.put("apiKey", config.getString("apiKey"))
    head_json.put("method", "request")
    head_json.put("timestamp", timestamp.toString)
    head_json.put("sign", sign)

    val par_json = new util.HashMap[String, Any]()
    par_json.put("authCode", config.getString("authCode"))
    if(config.getValueByValueNonEmpty("apiType").nonEmpty && !config.getString("apiType").equals("10004")){
      par_json.put("isTest", "false")
    }

//        val conf = env.getSparkSession.sparkContext.broadcast(
//          Map[String, Object](
//            "bootstrap.servers" -> "192.168.1.112:9092",
//            "key.serializer" -> classOf[StringSerializer].getName,
//            "value.serializer" -> classOf[StringSerializer].getName))

    val sendStrAry = env.getSparkSession.sparkContext.collectionAccumulator[String]("sendMethod")
    val schema: StructType = data.schema
    data.foreachPartition(partition => {
      partition.foreach(p => {
        val req_map = new util.HashMap[String, Object]()
        schema.fields.foreach(sc => {
          req_map.put(sc.name, p.getAs(sc.name))
        })

        val requestFiledsMap = new util.HashMap[String, Any]()
        if(config.getValueByValueNonEmpty("apiType").nonEmpty && !config.getString("apiType").equals("10004")) {
          requestFiledsMap.put("request_fileds", req_map)
        }

        // 加密和非加密
        val map: util.HashMap[String, _ >: Object] = if (requestFiledsMap.isEmpty) req_map else requestFiledsMap
        config.getString("encrypt_transfer").toLowerCase() match {
          case "1" => par_json.put("reqParams", RSAUtil.publicEncrypt(JSON.toJSONString(map,SerializerFeature.BeanToArray).getBytes(), RSAUtil.string2PublicKey(config.getString("pub_secret"))))
          case "0" => par_json.put("reqParams", map)
          case _ => throw new Exception("传入的 encrypt_transfer 只能是 1 和 0, 请检查 encrypt_transfer 值。")
        }

        head_json.put("params", par_json)

        val sendJsonStr = JSON.toJSONString(head_json, SerializerFeature.BeanToArray)
        println(sendJsonStr)

        // http 请求调用 Api 服务
        val str = postResponse(config.getString("url"), sendJsonStr)

        if(str != null) {
          val sendMap: JSONObject = new JSONObject()
          sendMap.put("receive", getReceiveJson())
          sendMap.put("send", sendJsonStr)
          sendMap.put("code", str.get("code"))
          sendMap.put("str", str)
  //        sinkKafka(sendStrAry, sendMap, conf)
          if((str.get("code").isInstanceOf[String]&&str.get("code").equals("200"))||(str.get("code").isInstanceOf[Int]&&str.get("code")==200)){
            sendMap.put("message","成功")
          }else {
            throw new Exception(s"Failed. The return status code is not 200，The current status code is ${str.get("code")}, send message: $sendJsonStr")
            // sendMap.put("message", "失败")
          }
          sendStrAry.add(sendMap.toJSONString)
        } else {
          throw new Exception("************ gateway is not responding... ")
        }
      })
    })
    //    sinkHdfs(sendStrAry, env)
//    sinkHive(sendStrAry, env)
  }

  def checkSchema(schema: StructType): Unit = {
    val str: Array[String] = config.getString("source_column").split(",")
    for(s <- str) {
      var flag = false
      schema.fields.foreach((sc: StructField) => {
        val str1 = sc.name
        if (str1.toLowerCase.equals(s.toLowerCase)) {
          flag = true
        }
      })
      if(!flag) throw new Exception("数据源字段不匹配")
    }

  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("url", "apiKey", "method", "signType", "authCode", "salt", "isTest", "source_column", "pub_secret","encrypt_transfer", "apiType")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.nonEmpty) {
      new CheckResult(false, "please specify " + nonExistsOptions.map { option =>
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

  // TODO
  def getReceiveJson() = {
    val requestMap = new JSONObject()
    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "", false)) match {
      case Success(options) => {
        options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            requestMap.put(entry.getKey, entry.getValue.unwrapped().toString)
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })
      }
      case Failure(exception) => // do nothing
    }

    requestMap.toJSONString
  }

  //TODO 从数据库获取 HDFS 路径
  def sinkHdfs(sendStrAry: CollectionAccumulator[String], env: SparkEnvironment): Unit ={
    val sendList = sendStrAry.value
    val sendSeq = JavaConverters.asScalaIteratorConverter(sendList.iterator()).asScala.toSeq
    val sendRDD = env.getSparkSession.sparkContext.parallelize(sendSeq)
    val sendSchema = StructType(Array(StructField("method", StringType, true)))
    val sendDF = env.getSparkSession.createDataFrame(sendRDD.map(Row(_)), sendSchema)
    val date = DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss").format(LocalDateTime.now())
    sendDF.write.mode(SaveMode.Overwrite).json(s"/kino/apisink/$date")
  }

  //TODO 从数据库获取 Hive 表
  def sinkHive(sendStrAry: CollectionAccumulator[String], env: SparkEnvironment): Unit ={
    val sendList = sendStrAry.value
    val sendSeq = JavaConverters.asScalaIteratorConverter(sendList.iterator()).asScala.toSeq
    val sendRDD = env.getSparkSession.sparkContext.parallelize(sendSeq)
    val sendSchema = StructType(Array(StructField("method", StringType, true)))
    val sendDF = env.getSparkSession.createDataFrame(sendRDD.map(Row(_)), sendSchema)
    val date = DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss").format(LocalDateTime.now())
    sendDF.write.mode("overwrite").format("Hive").saveAsTable("default.testkinotab")
    //    sendDF.write.mode(SaveMode.Overwrite).json(s"/kino/apisink/$date")
  }

  //TODO 从数据库获取 Kafka Topic
//  def sinkKafka(sendStrAry: CollectionAccumulator[String], sendMap: JSONObject, conf: Broadcast[Map[String, Object]]): Unit ={
//    val sendJsonString = sendMap.toJSONString
//    sendStrAry.add(sendJsonString)
//    val sink: KafkaSink[String, Object] = KafkaSink[String, Object](conf.value)
//    sink.send("tc_test", sendJsonString)
//  }

  def getSqlAliasChange(ds: Dataset[Row], sql: String, spark: SparkSession): Dataset[Row] = {
    ds.createOrReplaceTempView("api")
    spark.sql(sql)
  }

  def getSign(timestamp: Long) = {
    val getSignMap = new util.HashMap[String, Object]()
    getSignMap.put("apiKey", config.getString("apiKey"))
    getSignMap.put("method", "request")
    getSignMap.put("timestamp", timestamp.toString)

    config.getString("signType").toLowerCase() match {
      // MD5 加密
      case "1" => MD5.encrypt(MD5.stringNormalSort(getSignMap), config.getString("salt"))
      // Sha256 加密
      case "3" => Sha256.getSignCommon(getSignMap, new util.ArrayList[String]())
      case _ => throw new Exception("传入的 sign 只能是 1 和 3, 请检查 sign 值。")
    }
  }

  /**
   * 发送 Post 请求方法
   *
   * @param url         : Api Url 地址
   * @param params      : 需要传的参数
   * @return
   */
  def postResponse(url: String, params: String = null)  = {
    println(s"******* connect gateway *******")
    println(s"******* gateway url: $url *******")
    println(s"******* send params: $params *******")
    val httpClient: DefaultHttpClient = new DefaultHttpClient()
//    val httpClient: HttpClients = new HttpClients()
//    val httpClient = HttpClients.createDefault()
    val post = new HttpPost(url)
    post.addHeader("Content-Type", "application/json")
    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }
    val response = httpClient.execute(post)
    val resultStr = EntityUtils.toString(response.getEntity, "UTF-8")
    println("******* resultStr **********"+resultStr)
    JSON.parseObject(resultStr)
  }
}

object Api {
  /**
   * 获取当前时间
   *
   * @param pattern pattern 如"yyyyMMddHHmmss"
   * @return
   */
  def getrealTime(pattern: String): String = {
    val timeTag = System.currentTimeMillis()
    val changeTime = new Date(timeTag)
    val dataFormat = new SimpleDateFormat(pattern)
    dataFormat.format(changeTime)
  }

  /**
   * 获取当前时间戳（精确到毫秒）
   *
   * @return
   */
  def getTimestamp(): Long = {
    val time = getrealTime("yyyyMMddHHmmss")
    funStringToTimeStamp(time, "yyyyMMddHHmmss")
  }

  /**
   * 将时间字符串修改为时间戳
   *
   * @param time          时间
   * @param timeFormatted 时间格式 如 "yyyyMMddHHmmss"
   * @return 精确到毫秒的时间戳
   */
  def funStringToTimeStamp(time: String, timeFormatted: String): Long = {
    val fm = new SimpleDateFormat(timeFormatted)
    val dt = fm.parse(time)
    dt.getTime
  }
}

