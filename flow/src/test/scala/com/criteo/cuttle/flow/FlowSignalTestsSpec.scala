package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.flow.signals.{EventSignal, SignalManager}
import com.criteo.cuttle.flow.utils.{KafkaConfig, KafkaMessage}
import com.criteo.cuttle.{Execution, Finished, ITTestScheduling, Job, Output}
import fs2.Stream
import io.circe.Json
import org.scalatest.Matchers
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import io.circe.syntax._
import fs2._


/**
  * @summary Functionals tests of the sub-library of scheduling `Flow` placed in [[com.criteo.cuttle.flow._]]
  *          - Testing signals
  *
  *           PS: Job's are not sophisticated, they are simple,
  *           they are here to assure that execution is conform at what we expect
  *
  * */
class FlowSignalTestsSpec extends FunSuite with ITTestScheduling with Matchers {

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  test("Signal sync") {

    val simpleWorkflow : WFSignalBuilder[String, String] = topic => {

      val job1 =    Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
        IO(Finished).unsafeToFuture()
      }

      val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
        e.streams.info("On step bis")
        val receive = for {
          value <- topic
            .subscribeOnTopic(e.context.workflowId)
            .head
            .compile
            .last
        } yield { Output(Json.obj("aud" -> s"step bis is done and received ${value.get._2}".asJson)) }

        receive.unsafeToFuture()
      }

      val job2 =    Job(s"step-two", FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
        val in = e.job.scheduling.inputs
        val x = e.optic.aud.string.getOption(in).get + " passed to step three"
        IO(Output(x.asJson)).unsafeToFuture()
      }

      (job1 && job1bis) --> job2
    }

    val kafkaCf = KafkaConfig("signal_cuttle", "signals", List("localhost:9092"))
    val program = for {
      // Initialisation
      signalManager <- Stream.eval(SignalManager[String, String](kafkaCf))
      // Setup Workflow
      workflowWithTopic = simpleWorkflow(signalManager)
      project <- Stream.eval(FlowCreator("test-signal-01", "Test of jobs signal")(workflowWithTopic))
      _ <-  Stream.eval(signalManager.newTopic(project.workflowId))
      // Run it
      res <- project.start().concurrently(signalManager.broadcastTopic)
       _  <- signalManager
          .pushOne(project.workflowId, "a message")
          .delayBy[IO](3.seconds)
          .flatMap(_ => Stream.eval(signalManager.removeTopic(project.workflowId)))

    } yield res

    val programExecutions = program.compile.toList.unsafeRunSync()
    programExecutions.length shouldBe 3 +- 4 // Depends on if it gets the result of both job1 and job1bis at the same time


  }

}


