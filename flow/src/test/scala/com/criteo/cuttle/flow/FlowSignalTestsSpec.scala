package com.criteo.cuttle.flow

import cats.effect.concurrent.Deferred
import io.circe.Json
import org.scalatest.Matchers
import org.scalatest.FunSuite
import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.flow.signals._
import com.criteo.cuttle.flow.utils._
import com.criteo.cuttle.{DatabaseConfig, Finished, ITTestScheduling, Job, Output, Database => CoreDB}

import scala.concurrent.{ExecutionContext}
import io.circe.syntax._


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
      transactor    <- xa
      stopping      <- Deferred[IO, Unit]
      signalManager <- SignalManager[String, String](kafkaCf)
      scheduler     <- FlowManager(transactor, signalManager)
      workflowWithTopic = simpleWorkflow(signalManager)

      graph1 <- FlowExecutor(transactor, "Run jobs with signal")(workflowWithTopic)
      graph2 <- FlowExecutor(transactor, "Run jobs with signal")(workflowWithTopic)

      flow1 <- scheduler.runOne(graph1).start
      flow2 <- scheduler.runOne(graph2).start

      runFlowAndStop = (flow1.join, flow2.join).parMapN { (resWorkflow1, resWorkflow2) =>
        (resWorkflow1, resWorkflow2)
      }.flatMap { res => stopping.complete(()).map(_ => res)}

      finalResult <- (runFlowAndStop,
            signalManager.broadcastTopic.interruptWhen(stopping.get.attempt).compile.drain,
            signalManager.pushOne(graph2.workflowId, "step-one-bis").compile.drain,
            signalManager.pushOne(graph1.workflowId, "step-one-bis").compile.drain)
        .parMapN { (bothWorkflow, _, _, _) => bothWorkflow }
    } yield finalResult

    val bothWorkflow = program.unsafeRunSync()
    val (flow1, flow2) = bothWorkflow

    flow1 should be ('right)
    flow2 should be ('right)

    val (flowWorkflow1, stateWF1) = flow1.toOption.get
    val (flowWorkflow2, stateWF2) = flow2.toOption.get

    stateWF1.keySet should contain theSameElementsAs(flowWorkflow1 verticesFrom RoutingKind.Success)
    stateWF1.values.toList should contain theSameElementsAs List.fill(flowWorkflow1.vertices.size)(Done)

    stateWF2.keySet should contain theSameElementsAs(flowWorkflow2 verticesFrom RoutingKind.Success)
    stateWF2.values.toList should contain theSameElementsAs List.fill(flowWorkflow2.vertices.size)(Done)

    val nodeFinalWF1 = flowWorkflow1 get "step-two"
    val nodeFinalWF2 = flowWorkflow2 get "step-two"

    val aud1 = nodeFinalWF1.get.scheduling.inputs.hcursor.downField("aud").as[String].isRight
    val aud2 = nodeFinalWF2.get.scheduling.inputs.hcursor.downField("aud").as[String].isRight

    assert(aud1 && aud2)

  }

}


