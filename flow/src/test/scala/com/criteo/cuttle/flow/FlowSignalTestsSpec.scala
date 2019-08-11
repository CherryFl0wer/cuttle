package com.criteo.cuttle.flow

import java.util.concurrent.Executors

import cats.effect.concurrent.Deferred
import fs2.Stream
import io.circe.Json
import org.scalatest.Matchers
import org.scalatest.FunSuite
import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow._
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.flow.signals._
import com.criteo.cuttle.flow.utils._
import com.criteo.cuttle.{DatabaseConfig, Finished, ITTestScheduling, Job, Output, Database => CoreDB}

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

  val xa = CoreDB.connect(DatabaseConfig.fromEnv)(logger)

  test("Signal sync") {

    val simpleWorkflow : WFSignalBuilder[String, String] = topic => {

      lazy val job1 =    Job("step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
        IO(Finished).unsafeToFuture()
      }

      lazy val job1bis = Job("step-one-bis", FlowScheduling()) { implicit execution =>
        execution.streams.info("On step bis")
        val receive = for {
          _ <- IO(println(s"Context of workflow ${execution.context.workflowId}"))
          value <- topic
            .subscribeOnTopic(execution.context.workflowId)
            .evalTap(m => IO(println(s"received $m")))
            .head
            .compile
            .last
        } yield { Output(Json.obj("aud" -> s"step bis is done and received ${value.get._2}".asJson)) }

        receive.unsafeToFuture()
      }

      lazy val job2 = Job("step-two", FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
        val in = e.job.scheduling.inputs
        val x = e.optic.aud.string.getOption(in).get + " passed to step three"
        IO(Output(x.asJson)).unsafeToFuture()
      }

      (job1 && job1bis) --> job2
    }

    val kafkaCf = KafkaConfig("signal_cuttle", "signals", List("localhost:9092"))

    import cats.implicits._

    val program = for {
      tc            <- xa
      stopping      <- Deferred[IO, Unit]
      signalManager <- SignalManager[String, String](kafkaCf)
      scheduler     <- WorkflowManager(tc, signalManager)(20)
      workflowWithTopic = simpleWorkflow(signalManager)

      graph1 <- FlowCreator(tc, "Run jobs with signal")(workflowWithTopic)
      graph2 <- FlowCreator(tc, "Run jobs with signal")(workflowWithTopic)

      fib1 <- scheduler.runOne(graph1).start
      fib2 <- scheduler.runOne(graph2).start

      runFlowAndStop = (fib1.join, fib2.join).parMapN { (wf1, wf2) =>
        (wf1, wf2)
      }.flatMap { res => stopping.complete(()).map(_ => res)}

      wf12 <- (runFlowAndStop,
            signalManager.broadcastTopic.take(2).interruptWhen(stopping.get.attempt).compile.drain,
            signalManager.pushOne(graph2.workflowId, "step-one-bis").compile.drain,
            signalManager.pushOne(graph1.workflowId, "step-one-bis").compile.drain)
        .parMapN { (wf12, _, _, _) => wf12 }
    } yield wf12

    val wf12 = program.unsafeRunSync()
    val (flow1, flow2) = wf12

    flow1 should be ('right)
    flow2 should be ('right)

    assert(true)
  }

}


