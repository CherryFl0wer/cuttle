package com.criteo.cuttle.flow.signals

import java.util

import fs2.Pure
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
  * Consumes and produces to/from Kafka
  * @todo analyze Signal object from fs2
  * @todo make it generic (can be something else than kafka)
  * @todo build with a kafka config
  */
class KafkaNotification(val kafkaConfig: KafkaConfig) {
  import scala.collection.JavaConverters._

  private val producer = new KafkaProducer[String, String](kafkaConfig.producerProperties)
  private val consumer = new KafkaConsumer[String, String](kafkaConfig.consumerProperties)

  private def subscribeTo(topic : String) = consumer.subscribe(util.Collections.singletonList(topic)) // Variadic

  def push(topic: String, key: String, content : String) =
    producer.send(new ProducerRecord[String, String](topic, key, content)).isDone


  def consume(onTopic : String, waitFor : Long = 0): fs2.Stream[Pure, ConsumerRecord[String, String]] = {
    subscribeTo(onTopic)

    val records: Stream[ConsumerRecord[String, String]] = consumer.poll(waitFor)
      .asScala.toStream

    fs2.Stream.emits(records)
  }
}
