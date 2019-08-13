package com.criteo.cuttle.flow.signals

import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.effect.concurrent.Ref
import com.criteo.cuttle.flow.utils.{KafkaConfig, KafkaMessage}
import fs2.{Pipe, Stream}
import fs2.concurrent.Topic
import fs2.kafka.{Deserializer, Serializer, consumerStream}

/** */
sealed trait EventSignal extends Serializable

// Define an empty signal
case object SigEmpty extends EventSignal

// Define a signal if you want to kill a job
case class SigKillJob(jobId : String) extends EventSignal

// Define a signal if you want to send data to a job
case class SigMessage[T](jobId : String, data : T) extends EventSignal

class SignalManager[K,V](sharedState : Ref[IO, SignalManager.TopicState[K, V]], kafkaConfig : KafkaConfig)
                        (implicit serializerK: Serializer[K],
                         deserializerK: Deserializer[K],
                         serializerV: Serializer[V],
                         deserializerV: Deserializer[V],
                         F: ConcurrentEffect[IO],
                         timer: Timer[IO]) extends KafkaMessage[K, V](kafkaConfig) {

  private val nbOfSignalInQueue = 100

  /**
   * Consume message from the kafkaConfig and send the message to the topic
   * according to the key of the message
   */
  def broadcastTopic(implicit csKafka : ContextShift[IO]): Stream[IO, Unit] =  for {
    consumer     <- consume(csKafka).broadcastTo(pushMessageToTopics, commitOffsetPipe)
  } yield consumer

  /**
    * Subscribe depending on the key in the topic kafka
    * @param id Key in the topic
   */
  def subscribeOnTopic(id : K): Stream[IO, (K, V)] = for {
    stateMapOfTopic <- Stream.eval(sharedState.get)
    topic = stateMapOfTopic(id)
    streamOfSignals <- topic
      .subscribe(nbOfSignalInQueue)
      .collect { case Some(msg) => msg }
      .map { msg => (msg.record.key(), msg.record.value()) }
  } yield streamOfSignals

  /**
    * Pipe to push message to the good topic according to the key
    * @return a Stream of Unit
    */

  private def pushMessageToTopics : Pipe[IO, KafkaMessage.Message[K, V], Unit] = event => {
    for {
      stateMapOfTopic <- Stream.eval(sharedState.get)
      msg <- event
      record  = msg.get.record
      publish <- Stream.eval(stateMapOfTopic
        .get(record.key()).fold(IO.unit) { topic =>
            topic.publish1(msg)
        })
    } yield publish
  }

  /**
    * Create a new Topic to send message to. Based on the key id
    * @param id Key in the kafka topic
    * @return A boolean of creation
    */
  def newTopic(id : K): IO[Unit] = for {
    topic <- Topic[IO, KafkaMessage.Message[K, V]](None)
    state <- sharedState.update(oldState => oldState + (id -> topic))
  } yield state

  /**
    * Remove a topic
    * @param id Key used at the creation of a new topic `newTopic(K)`
    * @return
    */
  def removeTopic(id : K) : IO[Unit] = for {
    state <- sharedState.get
    updatedState = state - id
    _ <- sharedState.update(_ => updatedState)
  } yield ()

}


object SignalManager {

  type TopicState[K, V] = Map[K, Topic[IO, KafkaMessage.Message[K, V]]]

  def apply[K, V](kafka: KafkaConfig)(implicit serializerK: Serializer[K],
                                            deserializerK: Deserializer[K],
                                            serializerV: Serializer[V],
                                            deserializerV: Deserializer[V],
                                            F: ConcurrentEffect[IO],
                                            timer: Timer[IO]): IO[SignalManager[K, V]] = for {
    sharedState <- Ref.of[IO, TopicState[K,V]](Map.empty)
  } yield new SignalManager[K,V](sharedState, kafka)
}