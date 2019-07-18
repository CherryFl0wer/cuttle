package com.criteo.cuttle.flow.signals

import cats.data.{OptionT}
import cats.effect._
import cats.effect.concurrent.{Ref}
import cats.implicits._
import fs2.{Pipe, Stream}
import fs2.concurrent.{SignallingRef, Topic}
import fs2.kafka._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext


/** */
sealed trait EventSignal extends Serializable

// Define an empty signal
case object SigEmpty extends EventSignal

// Define a signal if you want to kill a job
case class SigKillJob(jobId : String) extends EventSignal

// Define a signal if you want to send data to a job
case class SigMessage[T](jobId : String, data : T) extends EventSignal



/**
  * Consumes from and produces data to Kafka
  * @todo analyze Signal object from fs2
  * @todo make it generic (can be something else than kafka)
  * @implicit Serializer and deserializer for Key and Value
  *           IO concurrent methods and timer
  */
class KafkaNotification[K, V](kafkaConfig : KafkaConfig, sharedState : Ref[IO, KafkaNotification.TopicState[K,V]])
                             (implicit
                              serializerK: Serializer[K],
                              deserializerK: Deserializer[K],
                              serializerV: Serializer[V],
                              deserializerV: Deserializer[V],
                              F: ConcurrentEffect[IO],
                              timer: Timer[IO]) {

  
  private val commitMessageEverySec = 10 seconds
  private val commitMessageEveryN = 10
  private val pullMessageFromKafkaEverySec = 1 seconds
  private val nbOfSignalInQueue = 100

  private val producerSettings = ProducerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withAcks(Acks.All)

  private val consumerSettings = ConsumerSettings[K,V]
    .withBootstrapServers(kafkaConfig.serversToString)
    .withGroupId(kafkaConfig.groupId)
    .withEnableAutoCommit(false)
    .withAutoOffsetReset(AutoOffsetReset.Latest)
    .withPollTimeout(pullMessageFromKafkaEverySec)

  private def commitOffsetPipe : Pipe[IO, KafkaNotification.Event[K,V], Unit] = event => (for {
    msg <- event
    offset = msg.get.committableOffset
  } yield offset).through(commitBatchWithin(commitMessageEveryN, commitMessageEverySec))

  private def pushMessageToTopics : Pipe[IO, KafkaNotification.Event[K,V], Unit] = event => {
    for {
      stateMapOfTopic <- Stream.eval(sharedState.get)
      msg <- event
      record = msg.get.record
      publish = stateMapOfTopic.get(record.key()).map { topic =>
        topic.publish1(msg)
      }
      _ <- Stream.eval(publish.traverse(identity))
    } yield ()
  }


  def sigRef = fs2.concurrent.SignallingRef[IO, Boolean](false)

  /***
     consume the topic
    * @param cs context shift io
    * @return  An infinite stream of consuming message in kafka.
    */
  def consumeFromKafka(implicit csKafka : ContextShift[IO], timerKafka : Timer[IO]): Stream[IO, Unit] =
    for {
      _ <- consumerStream[IO].using(consumerSettings)(csKafka, timerKafka)
        .evalTap(_.subscribeTo(kafkaConfig.topic))
        .flatMap(_.stream)
        .map(Some(_))
        .broadcastTo(pushMessageToTopics, commitOffsetPipe)
    } yield ()


  /**
    * @param predicate filter the topic with messages corresponding
    * @return
    */
  /*
  def subscribeOn(predicate : ConsumerRecord[K,V] => Boolean): Stream[IO, CommittableMessage[IO, K, V]] = for {
    msg <- topicEvents
      .subscribe(nbOfSignalInQueue)
      .collect { case Some(msg) => msg }
      .filter(ev => predicate(ev.record))
  } yield msg

  def subscribeToKey(key : K) = for {
    msg <- topicEvents
      .subscribe(nbOfSignalInQueue)
      .collect { case Some(msg) => msg }
      .filter(m => m.record.key() == key )
      .map(_.record.value())
  } yield msg */


  def subscribeOnTopic(id : K): Stream[IO, (K, V)] = for {
    stateMapOfTopic <- Stream.eval(sharedState.get)
    topic = stateMapOfTopic(id)
    streamOfSignals <- topic
      .subscribe(nbOfSignalInQueue)
      .collect { case Some(msg) => msg }
      .map { msg => (msg.record.key(), msg.record.value()) }
  } yield streamOfSignals


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


  def newTopic(id : K): IO[Boolean] = for {
    topic <- Topic[IO, KafkaNotification.Event[K,V]](None)
    state <- sharedState.tryUpdate(oldState => oldState + (id -> topic))
  } yield state


}

object KafkaNotification {

  type Event[K, V] = Option[CommittableMessage[IO, K, V]]
  type TopicState[K, V] = Map[K, Topic[IO, Event[K, V]]]

  def apply[K,V](kafkaConfig: KafkaConfig)(implicit serializerK: Serializer[K],
                                           deserializerK: Deserializer[K],
                                           serializerV: Serializer[V],
                                           deserializerV: Deserializer[V],
                                           F: ConcurrentEffect[IO],
                                           timer: Timer[IO]): IO[KafkaNotification[K, V]] = for {
     sharedState <- Ref.of[IO, TopicState[K,V]](Map.empty)
  } yield new KafkaNotification[K,V](kafkaConfig, sharedState)

}