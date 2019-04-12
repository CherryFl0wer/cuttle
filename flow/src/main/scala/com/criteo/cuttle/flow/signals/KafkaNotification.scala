package com.criteo.cuttle.flow.signals

import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Topic
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

  private val producerSettings = ProducerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withAcks(Acks.All)

  private val consumerSettings = ConsumerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withGroupId(kafkaConfig.groupId)
    .withEnableAutoCommit(false)
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withPollTimeout(250.millisecond)


  def subscribeTo(predicate : (ConsumerRecord[K,V]) => Boolean)(implicit C : Concurrent[IO]) =
    topicEvents
      .subscribe(10)
      .collect { case Right(msg) => msg }
      .filter(ev => predicate(ev.record))
     .evalTap(ev => IO.delay(println(s"Received ${ev.record.key} with val ${ev.record.value}")))


  /***
    * pushOne data to the topic
    * @param data tuple of Key, Value
    * @return An IO of a producer result
    */
  def pushOne(data : (K,V)) = (for {
    producer <- producerStream[IO].using(producerSettings)
    record = ProducerRecord(kafkaConfig.topic, data._1, data._2)
    msg    = ProducerMessage.one(record)
    result <- Stream.eval(producer.produce(msg).flatten)
  } yield result).compile.lastOrError


  /***
    consume the topic
    * @param cs context shift io
    * @return An IO to execute the consumer stream
    */
  def consume(implicit cs : ContextShift[IO]) = for {
    _ <- consumerStream[IO]
      .using(consumerSettings)
      .evalTap(_.subscribeTo(kafkaConfig.topic))
      .flatMap(_.stream)
      .evalMap(ev => topicEvents.publish1(Right(ev)))
  } yield ()







}