package com.criteo.cuttle.examples

import cats.effect.{ExitCode, IO, IOApp}
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification}
import fs2.Stream
import scala.concurrent.duration._
import cats.implicits._

object TestFS2 extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val rnd = scala.util.Random

    val flowSignalTopic = new KafkaNotification[String, String](KafkaConfig(
      topic = "flow-signal-topic",
      groupId = "flow-signal",
      servers = List("localhost:9092")))


    Stream(
      flowSignalTopic
        .consume
        .concurrently(
        Stream.awakeEvery[IO](3 seconds).evalMap { n =>
          flowSignalTopic.pushOne((s"workflow-rnd-${rnd.nextInt(2)}", s"signal-event-${n._1.toString}"))
        })).parJoin(2).compile.drain.unsafeRunAsyncAndForget()


    Stream(flowSignalTopic.subscribeTo(record => record.key == "workflow-rnd-1"))
      .concurrently(flowSignalTopic.subscribeTo(record => record.key == "workflow-rnd-1"))
      .parJoin(2).compile.drain.as(ExitCode.Success)

    }
}

