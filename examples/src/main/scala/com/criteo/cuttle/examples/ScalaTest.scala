package com.criteo.cuttle.examples

import cats.effect._
import cats.effect.concurrent.Deferred
import cats.implicits._
import fs2.Stream
import fs2.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord

object ScalaTest extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO(println(s"Processing record: $record"))

    val consumerSettings =
      ConsumerSettings[String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("signals")

    val defe: IO[Deferred[IO, Unit]] = Deferred[IO, Unit]
    val resource: Resource[IO, KafkaConsumer[IO, String, String]] = consumerResource(consumerSettings)
    val value: fs2.Stream[IO, KafkaConsumer[IO, String, String]] = Stream.resource(resource)

    defe.flatMap((myd: Deferred[IO, Unit]) =>
      {


        val stream =
          value
            .evalTap(_.subscribeTo("signal_cuttle"))
            .flatMap(_.partitionedStream)
            .map { partitionStream =>
              partitionStream
                .evalMap { committable =>
                  processRecord(committable.record) *> myd.complete(())
                }
            }
            .parJoinUnbounded.interruptWhen(myd.get.attempt)

        stream.compile.drain.as(ExitCode.Success)

      }
    )

  }
}
