package com.criteo.cuttle.examples

import java.util.concurrent.Executors

import cats.effect.{ExitCode, IO, IOApp}
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification}
import cats.implicits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


object TestFS2 extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val topic = "flow-signal-topic"

    val jobNotificationService = new KafkaNotification[String](KafkaConfig(
      groupId = "flow-signal",
      servers = List("localhost:9092")))


    val ecTwo = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    val csTwo = IO.contextShift(ecTwo)


    jobNotificationService.consume(topic).unsafeRunAsyncAndForget()
    jobNotificationService.printQueue(csTwo).compile.drain.unsafeRunAsyncAndForget()

    val toRun = fs2.Stream.awakeEvery[IO](1.seconds).evalMap {
      _ =>
        jobNotificationService.pushOne(topic, (s"workflow-rnd", s"signal-event"))
    }.compile.drain.unsafeRunSync()


    IO(ExitCode.Success)
  }
}