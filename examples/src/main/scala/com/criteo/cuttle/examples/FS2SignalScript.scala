package com.criteo.cuttle.examples

import cats.Monad
import cats.effect.{ExitCode, IO, IOApp, Timer}
import com.criteo.cuttle.{Job, Output}
import com.criteo.cuttle.flow.{FlowGraph, FlowScheduling, FlowWorkflow}
import com.criteo.cuttle.flow.signals._
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future, duration }

object FS2SignalScript extends IOApp {

  import io.circe.syntax._
  import fs2._

  type WFBuilder[K,V] = KafkaNotification[K,V] => FlowWorkflow

  def run(args: List[String]): IO[ExitCode] = {
    implicit val ctx = ExecutionContext.global

    val workflow1 : WFBuilder[String, String] = topic => {

      val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
        val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get + " passed to step two"

        val send = for {
          _ <- topic.pushOne("wf-1", "step-one").compile.drain
        } yield {
          Output(Json.obj("aud" -> x.asJson))
        }

        send.unsafeToFuture()
      }

      val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
        e.streams.info("On step bis")

        val receive = for {
          _ <- topic
            .subscribeOn(msg => msg.key() == "wf-1" && msg.value() == "step-one")
            .evalTap(_ => IO(println("Got it")))
            .head
            .compile
            .last
        } yield {
          Output(Json.obj("aud" -> "step is bis".asJson))
        }
        receive.unsafeToFuture()
      }

      val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
        val in = e.job.scheduling.inputs
        val x = e.optic.aud.step_one_bis.string.getOption(in).get + " passed to step three"

        println("on it 2")
        Future {
          Output(x.asJson)
        }
      }

      (job1 && job1bis) --> job2
    }

    val program = for {
      signalManager <- Stream.eval(KafkaNotification[String, String](KafkaConfig("cuttle_message", "cuttlemsg", List("localhost:9092"))))
      workflowWithTopic = workflow1(signalManager)
      project <- Stream.eval(FlowGraph("test03", "Test of jobs I/O")(workflowWithTopic))
      browse <- project.start().concurrently(signalManager.consume)
    } yield browse


    program.compile.toList.map(_.foreach(println)).unsafeRunSync()
    IO(ExitCode.Success)
  }
}
