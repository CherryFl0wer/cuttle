package com.criteo.cuttle.examples

import cats.data.OptionT
import cats.effect._
import cats.effect.concurrent.Deferred
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.{DatabaseConfig, Finished, Job, Output, OutputErr}
import com.criteo.cuttle.flow.{FlowExecutor, FlowManager, FlowScheduling, FlowWorkflow}
import com.criteo.cuttle.flow.signals._
import com.criteo.cuttle.flow.utils.KafkaConfig
import io.circe.Json


object StartingScript extends IOApp {

  import io.circe.syntax._
  import fs2._

  val workflow1 : WFSignalBuilder[String, String] = topic => {
    val job1    = Job(s"step-one",     FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get + " passed to step two"
      IO(Output(Json.obj("result" -> x.asJson))).unsafeToFuture()
    }

    val errJob = Job("error-job", FlowScheduling()) { implicit e =>
      val errors = e.optic.err.string.getOption(e.job.scheduling.inputs).get
      e.streams.error(errors)
      IO(Finished).unsafeToFuture()
    }

    val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
      e.streams.info("On step bis")
      val receive = for {
        value <- topic
          .subscribeOnTopic(e.context.workflowId)
          .evalTap(ll => IO(println(s"Received a message for ${ll._1}")))
          .head
          .compile
          .last
        _ <- IO(println(s"Received $value"))
      } yield {
        if (value.isEmpty)
          OutputErr(Json.obj("err" -> "Oh no uwu".asJson))
        else
          Output(Json.obj("res" -> "coming from job1 bis".asJson))
      }
      receive.unsafeToFuture()
    }

    val job2    = Job(s"step-two",  FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      val x = e.optic.result.string.getOption(in).fold("No value")(identity) + " got it"
      val tenerifie = e.optic.tenerifie.string.getOption(in).fold("No value")(identity)


      IO(Output(Json.obj("exploded" -> x.asJson, "possible" -> tenerifie.asJson))).unsafeToFuture()
    }

    (job1.error(errJob) && job1bis.error(errJob)) --> job2
  }

  import com.criteo.cuttle.{ Database => CoreDB }
  def run(args: List[String]): IO[ExitCode] = {

    val kafkaConfig = KafkaConfig("signal_cuttle", "cuttlemsg", List("localhost:9092"))

    import cats.implicits._

    val program = for {
      transactor    <- CoreDB.connect(DatabaseConfig.fromEnv)(logger) // connect to the database
      stopping      <- Deferred[IO, Unit]
      signalManager <- SignalManager[String, String](kafkaConfig)
      scheduler     <- FlowManager(transactor, signalManager)
      workflowWithTopic = workflow1(signalManager)


      graph1 <-  FlowExecutor(transactor, "Run jobs with signal")(workflowWithTopic)
      flow1 <- scheduler.runOne(graph1).start

      runFlowAndStop = flow1.join.flatMap { res => stopping.complete(()).map(_ => res) }

      finalResult <- (runFlowAndStop,
        signalManager.broadcastTopic.interruptWhen(stopping.get.attempt).compile.drain
      ).parMapN { (workflow, _) => workflow }

      workflow <- IO.fromEither(finalResult)
      (workflowEnded, _) = workflow

      graphStepTwo <- FlowExecutor(transactor, "rerun from step two")(workflowEnded)
      runStepTwo   <- scheduler.runJobFromFlow("step-two", graphStepTwo).start
      stepTwoRes   <- runStepTwo.join

      runStepTwoAgain <- scheduler.runJobFromWfId("step-two", graph1.workflowId, workflowWithTopic, Some(Json.obj(
        "tenerifie" -> "sea".asJson,
        "calcul" -> 4.asJson
      )))

    } yield {
      if (runStepTwoAgain.isLeft) ExitCode.Error
      else ExitCode.Success
    }

    program
  }
}
