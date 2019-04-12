package com.criteo.cuttle.flow.signals

import cats.effect._
import cats.implicits._
import fs2.concurrent.Queue
import fs2.kafka._

import scala.concurrent.duration._
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * Consumes and produces to/from Kafka
  * @todo analyze Signal object from fs2
  * @todo make it generic (can be something else than kafka)
  * @source https://ovotech.github.io/fs2-kafka/docs/quick-example
  * @implicit Serializer and deserializer for Key and Value
  */

class KafkaNotification[V](val kafkaConfig : KafkaConfig)
                                                 (implicit
                                                  serializerV: Serializer[V],
                                                  deserializerV: Deserializer[V],
                                                  F : ConcurrentEffect[IO],
                                                  timer : Timer[IO]) {

  type Event = CommittableMessage[IO, String, V]

  //private val queue = fs2.Stream.eval(Queue.unbounded[IO, Event])
  private val kq = Queue.unbounded[IO, Event].unsafeRunSync()

  private val producerSettings = ProducerSettings[String,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withAcks(Acks.All)

  private val consumerSettings = ConsumerSettings[String,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withGroupId(kafkaConfig.groupId)
    .withEnableAutoCommit(false)
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withPollTimeout(250.millisecond)


  def pushOne(topic: String, data : (String,V)) = (for {
    producer <- producerStream[IO].using(producerSettings)
    record = ProducerRecord(topic, data._1, data._2)
    msg    = ProducerMessage.one(record)
    result <- fs2.Stream.eval(producer.produce(msg).flatten)
  } yield result).compile.lastOrError



  def printQueue(implicit csPrinter : ContextShift[IO]) =
    kq.dequeue.evalMap { ev =>
      F.delay(println(s"${ev.record.key} -> ${ev.record.value}"))
    }


  def consume(topic: String)(implicit cs : ContextShift[IO]) = (for {
    _ <- consumerStream[IO]
      .using(consumerSettings)
      .evalTap(_.subscribeTo(topic))
      .flatMap(_.stream)
      .evalMap(kq.enqueue1)
  } yield ()).compile.drain







}