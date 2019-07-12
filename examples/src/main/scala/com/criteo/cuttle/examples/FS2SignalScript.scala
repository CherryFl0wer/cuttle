package com.criteo.cuttle.examples

import cats.data.OptionT
import cats.effect.{ExitCode, IO, IOApp}
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.{Finished, Job, Output}
import com.criteo.cuttle.flow.{FlowGraph, FlowScheduling}
import com.criteo.cuttle.flow.signals._
import io.circe.Json

object FS2SignalScript extends IOApp {

  import io.circe.syntax._
  import fs2._


  def run(args: List[String]): IO[ExitCode] = {
    import scala.concurrent.duration._

    val workflow1 : WFSignalBuilder[String, String] = topic => {

      val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
        val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get + " passed to step two"
        IO(Finished).unsafeToFuture()
      }

      val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
        e.streams.info("On step bis")

        val receive = for {
          value <- topic
            .subscribeOn(msg => msg.key() == "wf-1" && msg.value() == "step-one")
            .head
            .compile
            .last
          _ <- IO(println(s"Received ${value}"))
        } yield { Output(Json.obj("aud" -> "step is bis".asJson)) }

        receive.unsafeToFuture()
      }

      val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
        val in = e.job.scheduling.inputs
        val x = e.optic.aud.string.getOption(in).get + " passed to step three"
        IO(Output(x.asJson)).unsafeToFuture()
      }

      (job1 && job1bis) --> job2
    }


    val program = for {
      signalManager <- Stream.eval(KafkaNotification[String, String](KafkaConfig("cuttle_message", "cuttlemsg", List("localhost:9092"))))
      workflowWithTopic = workflow1(signalManager)
      project <- Stream.eval(FlowGraph("test03", "Test of jobs I/O")(workflowWithTopic))

      res <- project.start().concurrently(fs2.Stream(
        signalManager.consume,
        signalManager.pushOne("wf-1", "step-one").delayBy[IO](10.seconds).map(_ => ()),
      ).parJoin(2))
    } yield res


    program.compile.toList.map(_.foreach(println)).unsafeRunSync()
    IO(ExitCode.Success)
  }
}
