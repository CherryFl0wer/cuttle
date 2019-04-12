package com.criteo.cuttle.examples

import cats.effect._
import com.criteo.cuttle.{Scheduling, _}
import com.criteo.cuttle.flow._
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification, SignallingJob}

import scala.concurrent.Future


object TestWorkflow extends IOApp {

  import com.criteo.cuttle.platforms.local._
  import io.circe.Json
  import io.circe.parser.parse
  import io.circe.syntax._

  def run(args: List[String]): IO[ExitCode] = {
    val jobNotificationService = new KafkaNotification[String, String](KafkaConfig(
      topic = "flow-signal-topic",
      groupId = "flow-signal",
      servers = List("localhost:9092")))

//    jobNotificationService.consume.unsafeRunAsyncAndForget()

    val machineLearningProject = FlowProject(description = "Testing code to implement flow workflow with signal") {
      booJob dependsOn (
        modellingJob dependsOn (
          dataprepJob and makeTrainJob and SignallingJob.kafkaSignaledJob("Enclenche", "desc", kafkaService = jobNotificationService)
          ) and (
          fooJob dependsOn makePredictionJob
          )
        )
    }

    machineLearningProject.start()

    //jobNotificationService.pushOne((machineLearningProject.workflowId, "declare-step")).unsafeRunSync()

    // then workflow should continue with modeling job

    IO(ExitCode.Success)
  }

  private val booJob = {
    Job("Step6", FlowScheduling(Some("{myparam1: \"ok\"}")), "Booing") {
      implicit e =>

        e.streams.info("Testing 6")
        val jsonInputs = e.job.scheduling.inputs.fold(Json.Null)(j => parse(j).right.getOrElse(Json.Null))

        e.context.result = Json.obj(
          "response" -> "as two".asJson,
          "inputsWas" -> jsonInputs
        )

        Future.successful(Completed)
    }
  }

  private val fooJob = {
    Job("Step5", FlowScheduling(), "Fooing") {
      implicit e: Execution[FlowScheduling] =>
        e.streams.info("Testing 5")
        Future.successful(Completed)
    }
  }

  private val modellingJob = {
    Job("Step4", FlowScheduling(), "modeling") {
      implicit e =>
        e.streams.info("Testing 4")
        Future.successful(Completed)
    }
  }

  private val makePredictionJob = {
    Job("Step3", FlowScheduling(), "predicting") {
      implicit e =>
        e.streams.info("Testing 3")
        e.context.resultsFromPreviousNodes.get("Step2").foreach(json => println(json))
        Future.successful(Completed)
    }
  }

  private val makeTrainJob = {
    Job("Step2", FlowScheduling(), "train") {
      implicit e =>
        e.streams.info("Testing 2")
        e.context.result = Json.obj(
          "name" -> "job2".asJson
        )
        Future.successful(Completed)
    }
  }

  private val dataprepJob = {
    Job("Step1", FlowScheduling(), "Data preparation") {
      implicit e =>
        exec"""sh -c '
         |    echo Looping for 10 seconds...
         |    for i in `seq 1 10`
         |    do
         |        date
         |        sleep 1
         |    done
         |    echo Ok
         |'""" ()

    }
  }
}
