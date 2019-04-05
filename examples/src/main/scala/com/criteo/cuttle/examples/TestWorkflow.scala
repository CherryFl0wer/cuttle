package com.criteo.cuttle.examples

import java.time.{Instant, LocalDate}
import java.time.ZoneOffset.UTC
import java.util.concurrent.Executors

import cats.effect._
import com.criteo.cuttle._
import com.criteo.cuttle.flow._

import scala.concurrent.Future
import com.criteo.cuttle.platforms.local._
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._

object TestWorkflow extends  IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val dataprepJob = Job("Step1", FlowScheduling(), "Data preparation") {
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
    val trainJob = Job("Step2", FlowScheduling(), "train") {
      implicit e =>

        e.streams.info("Testing 2")
        e.context.result = Json.obj(
          "name" -> "job2".asJson
        )

        Future.successful(Completed)
    }
    val predictionJob = Job("Step3", FlowScheduling(), "predicting") {
      implicit e =>

        e.streams.info("Testing 3")
        e.context.resultsFromPreviousNodes.get("Step2").foreach(json => println(json))
        Future.successful(Completed)
    }
    val signalledJob = Job("Enclenche",  FlowScheduling(), "wait for event kafka declare-step3", SignalJob(List("declare-step3"))) {
      implicit e => Future.successful(Completed)
    }
    val modellingJob = Job("Step4", FlowScheduling(), "modeling") {
      implicit e =>
        e.streams.info("Testing 4")

        Future.successful(Completed)
    }
    val fifthJob = Job("Step5", FlowScheduling(), "Fooing") {
      implicit e: Execution[FlowScheduling] =>
        e.streams.info("Testing 5")
        Future.successful(Completed)
    }
    val sixthJob = Job("Step6", FlowScheduling(Some("{myparam1: \"ok\"}")), "Booing") {
      implicit e =>

        e.streams.info("Testing 6")
        val jsonInputs =  e.job.scheduling.inputs.fold(Json.Null)(j => parse(j).right.getOrElse(Json.Null))

        e.context.result = Json.obj(
            "response" -> "as two".asJson,
            "inputsWas" -> jsonInputs
        )

        Future.successful(Completed)
    }

    val machineLearingProject = FlowProject(description = "Testing code to implement flow workflow with signal") {
      val left = modellingJob dependsOn (dataprepJob and trainJob and signalledJob)
      val right = fifthJob dependsOn predictionJob
      sixthJob dependsOn ( left and right )
    }
    machineLearingProject.start()


    val project2 = FlowProject(description = "Testing code to implement flow workflow 2") {
      sixthJob dependsOn (modellingJob and fifthJob) dependsOn predictionJob dependsOn (dataprepJob and trainJob)
    }
    
    //project2.start()

   // PushFlowSignal("declare-step3", project.workflowId)

    IO.pure(ExitCode.Success)
  }
}
