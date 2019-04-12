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


    val backend = for {
      consumer <- Stream(flowSignalTopic.consume)
      pusher   = Stream.awakeEvery[IO](1.seconds).evalMap {  n =>
        flowSignalTopic.pushOne((s"workflow-rnd-${rnd.nextInt(2)}", s"signal-event-${n._1.toString}"))
      }
      _ <- Stream(consumer.concurrently(pusher)).parJoin(2)

    } yield ()

    backend.compile.drain.unsafeRunAsyncAndForget()


    Stream(flowSignalTopic.subscribeTo(record => record.key == "workflow-rnd-1"))
      .concurrently(flowSignalTopic.subscribeTo(record => record.key == "workflow-rnd-0"))
      .parJoin(2)
      .compile
      .drain
      .as(ExitCode.Success)

    }
}

