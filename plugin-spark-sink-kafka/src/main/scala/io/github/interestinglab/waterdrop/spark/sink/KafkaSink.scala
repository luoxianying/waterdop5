package io.github.interestinglab.waterdrop.spark.sink

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.concurrent.Future
import scala.collection.JavaConversions._
import scala.collection.Map


/**
 * Created by kino on 2021/2/19.
 */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, key, value))
  }

  def send(topic: String, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, value))
  }
}

object KafkaSink {

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }
}