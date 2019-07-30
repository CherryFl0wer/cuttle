package com.criteo.cuttle.flow.utils

import cats.effect._
import cats.implicits._
import fs2.kafka._
import fs2.{Pipe, Stream}

import scala.concurrent.duration._

/**
  * @todo add doc
  * Consumes from and produces data to Kafka
  * implicit are Serializer and deserializer for Key and Value, IO concurrent methods and timer.
  */
class KafkaMessage[K, V](kafkaConfig : KafkaConfig)
                        (implicit serializerK: Serializer[K],
                         deserializerK: Deserializer[K],
                         serializerV: Serializer[V],
                         deserializerV: Deserializer[V],
                         F: ConcurrentEffect[IO],
                         timer: Timer[IO]) {


  protected val commitMessageEverySec: FiniteDuration = 5 seconds
  protected val commitMessageEveryN: Int = 5
  protected val pullMessageFromKafkaEverySec: FiniteDuration = 1 seconds

  protected val producerSettings: ProducerSettings[K, V] = ProducerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withAcks(Acks.All)

  protected val consumerSettings: ConsumerSettings[K, V] = ConsumerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withGroupId(kafkaConfig.groupId)
    .withEnableAutoCommit(false)
    .withAutoOffsetReset(AutoOffsetReset.Latest)
    .withPollTimeout(pullMessageFromKafkaEverySec)

  protected def commitOffsetPipe : Pipe[IO, KafkaMessage.Message[K,V], Unit] = event => (for {
    msg <- event
    offset = msg.get.committableOffset
  } yield offset).through(commitBatchWithin(commitMessageEveryN, commitMessageEverySec))

  /***
     consume the topic
    * @param csKafka context shift io
    * @return An infinite stream of consuming message in kafka.
    */
  def consume(implicit csKafka : ContextShift[IO])
  : Stream[IO, Some[CommittableMessage[IO, K, V]]] =
    for {
      consumer     <- consumerStream[IO].using(consumerSettings)(csKafka, timer)
        .evalTap(_.subscribeTo(kafkaConfig.topic))
        .flatMap(_.stream)
        .map(Some(_))
    } yield consumer


  /***
    pushOne data to the topic kafka
    * @param key Key in topics
    * @param value Value in topics
    * @return A Stream containing of IO with the result
    */
  def pushOne(key : K, value : V): Stream[IO, ProducerResult[Id, K, V, Unit]] = for {
    producer <- producerStream[IO].using(producerSettings)
    record = ProducerRecord(kafkaConfig.topic, key, value)
    msg    = ProducerMessage.one(record)
    result <- Stream.eval(producer.produce(msg).flatten)
  } yield result


}

object KafkaMessage {

  type Message[K, V] = Option[CommittableMessage[IO, K, V]]

  def apply[K,V](kafkaConfig: KafkaConfig)(implicit serializerK: Serializer[K],
                                           deserializerK: Deserializer[K],
                                           serializerV: Serializer[V],
                                           deserializerV: Deserializer[V],
                                           F: ConcurrentEffect[IO],
                                           timer: Timer[IO]): Stream[IO, KafkaMessage[K, V]] =
    Stream(new KafkaMessage[K,V](kafkaConfig)).covary[IO]

}