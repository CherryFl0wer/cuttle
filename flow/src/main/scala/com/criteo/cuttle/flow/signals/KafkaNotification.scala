package com.criteo.cuttle.flow.signals

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Topic
import fs2.kafka._

import scala.concurrent.duration._
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * Consumes from and produces data to Kafka
  * @todo analyze Signal object from fs2
  * @todo make it generic (can be something else than kafka)
  * @implicit Serializer and deserializer for Key and Value
  *           IO concurrent methods and timer
  */


class KafkaNotification[K, V](val kafkaConfig : KafkaConfig)
                                                 (implicit
                                                  serializerK: Serializer[K],
                                                  deserializerK: Deserializer[K],
                                                  serializerV: Serializer[V],
                                                  deserializerV: Deserializer[V],
                                                  F : ConcurrentEffect[IO],
                                                  timer : Timer[IO]) {

  type Event = Either[Unit, CommittableMessage[IO, K, V]]

  private val topicEvents = Topic[IO, Event](Left(())).unsafeRunSync()
  private val subscriber = topicEvents.subscribe(10).collect { case Right(msg) => msg }

  private val producerSettings = ProducerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withAcks(Acks.All)

  private val consumerSettings = ConsumerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withGroupId(kafkaConfig.groupId)
    .withEnableAutoCommit(false)
    .withAutoOffsetReset(AutoOffsetReset.Latest)
    .withPollTimeout(500.millisecond)

  def pushCommit(c : CommittableMessage[IO, K, V]) = c.committableOffset.commit

  def subscribeTo(predicate : (ConsumerRecord[K,V]) => Boolean) =
    subscriber
      .evalTap(ev => IO.delay(println(s"Key = ${ev.record.key()} ; Message = ${ev.record.value()}")))
      .filter(ev => predicate(ev.record))




  /***
    * pushOne data to the topic
    * @param data tuple of Key, Value
    * @return A Stream containing an IO with the result
    */
  def pushOne(data : (K,V)) = for {
    producer <- producerStream[IO].using(producerSettings)
    record = ProducerRecord(kafkaConfig.topic, data._1, data._2)
    msg    = ProducerMessage.one(record)
    result <- Stream.eval(producer.produce(msg).flatten)

  } yield result


  /***
    consume the topic
    * @param cs context shift io
    * @return A Stream of IO to execute the consumer stream
    */
  def consume(implicit cs : ContextShift[IO]) = for {

    _ <- consumerStream[IO]
      .using(consumerSettings)
      .evalTap(_.subscribeTo(kafkaConfig.topic))
      .flatMap(_.stream)
      .map(Right(_))
      .through(topicEvents.publish)
  } yield ()






}