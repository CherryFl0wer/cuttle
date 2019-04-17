package com.criteo.cuttle.examples

import cats.effect.{Async, ExitCode, IO, IOApp}
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification}
import fs2.Stream
import cats.implicits._

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._


object ExampleFS2 extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val rnd = scala.util.Random

    val flowSignalTopic: KafkaNotification[String, String] = new KafkaNotification[String, String](KafkaConfig(
      topic = "flow-signal-topic",
      groupId = "flow-signal",
      servers = List("localhost:9092"),
      offsetInterval = (500, 10.seconds)))


    val promise = Promise[String]()

    val backend: Stream[IO, Unit] = for {
      consumer <- Stream(flowSignalTopic.consume)
      pusher   = Stream.awakeEvery[IO](8.seconds).evalMap {  n =>
        flowSignalTopic.pushOne((s"workflow-3", s"signal-event")).compile.last
      }
      _ <- Stream(consumer.concurrently(pusher)).parJoin(2)

    } yield ()

    backend.compile.drain.unsafeRunAsyncAndForget()


    //flowSignalTopic.subscribeTo(record => record.key == "workflow-rnd-17").compile.toVector.
    flowSignalTopic
      .subscribeTo(record => record.key == "workflow-3" && record.value == "signal-event")
      .compile
      .last
      .unsafeRunAsync(cb => cb match {
        case Left(err) => promise failure(err)
        case Right(_) => promise success("received")
      })


    IO(ExitCode.Success)
  }
}

