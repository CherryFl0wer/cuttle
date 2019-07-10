package com.criteo.cuttle.flow.signals

import cats.effect._
import cats.implicits._
import fs2.Stream
import fs2.concurrent.{SignallingRef, Topic}
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


sealed trait EventSignals
case object SigEmpty extends EventSignals
case class SigKillJob(job : String) extends EventSignals
case class SigMessage[T](data : T) extends EventSignals


class KafkaNotification[K, V](kafkaConfig : KafkaConfig, topicEvents : Topic[IO, KafkaNotification.Event[K,V]])
                             (implicit
                              serializerK: Serializer[K],
                              deserializerK: Deserializer[K],
                              serializerV: Serializer[V],
                              deserializerV: Deserializer[V],
                              cs: ContextShift[IO],
                              F: ConcurrentEffect[IO],
                              timer: Timer[IO]) {


  private val consumer = consumerStream[IO]
    .using(consumerSettings)
    .evalTap(_.subscribeTo(kafkaConfig.topic))

  private val producerSettings = ProducerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withAcks(Acks.All)

  private val consumerSettings = ConsumerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withGroupId(kafkaConfig.groupId)
    .withEnableAutoCommit(true)
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withPollTimeout(500.millisecond)

  /***
    consume the topic
    * @param cs context shift io
    * @return A Stream of IO to execute the consumer stream
    */

  // Used for testing version
  def consumeAll() = consumerStream[IO]
    .using(consumerSettings)
    .evalTap(_.subscribeTo(kafkaConfig.topic))
    .flatMap(_.stream)
    .evalTap(ms => IO(println(ms)))



  def consume = for {
    _ <- consumerStream[IO]
      .using(consumerSettings)
      .evalTap(_.subscribeTo(kafkaConfig.topic))
      .flatMap(_.stream)
      .map(Some(_))
      .evalTap(ms => IO(println(ms)))
      .through(topicEvents.publish)
  } yield ()

  /**
    *
    * @param predicate filter the topic with messages corresponding
    * @return
    */
  def subscribeOn(predicate : ConsumerRecord[K,V] => Boolean) = for {
    msg <- topicEvents.subscribe(20)
      .collect { case Some(msg) => msg }
      .evalTap(ms => IO(println(ms)))
      .filter(ev => predicate(ev.record))
  } yield msg


  /***
    pushOne data to the topic
    * @param key Key in topics
    * @param value Value in topics
    * @return A Stream containing an IO with the result
    */
  def pushOne(key : K, value : V) = for {
    producer <- producerStream[IO].using(producerSettings)
    record = ProducerRecord(kafkaConfig.topic, key, value)
    msg    = ProducerMessage.one(record)
    result <- Stream.eval(producer.produce(msg).flatten)
  } yield result

}

object KafkaNotification {

  type Event[K, V] = Option[CommittableMessage[IO, K, V]]

  def apply[K,V](kafkaConfig: KafkaConfig)(implicit serializerK: Serializer[K],
                                           deserializerK: Deserializer[K],
                                           serializerV: Serializer[V],
                                           deserializerV: Deserializer[V],
                                           cs: ContextShift[IO],
                                           F: ConcurrentEffect[IO],
                                           timer: Timer[IO]) = for {
     topic <- Topic[IO, Event[K, V]](None)
  } yield new KafkaNotification[K,V](kafkaConfig, topic)
}