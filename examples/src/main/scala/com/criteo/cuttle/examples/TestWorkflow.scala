package com.criteo.cuttle.examples

import cats.effect._
import cats.implicits._

import com.criteo.cuttle._
import com.criteo.cuttle.flow._
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification, SignallingJob}

import scala.concurrent.Future
import scala.concurrent.duration._


object TestWorkflow extends IOApp {

  import com.criteo.cuttle.platforms.local._
  import io.circe.Json
  import io.circe.parser.parse
  import io.circe.syntax._

  private val flowSignalTopic = new KafkaNotification[String, String](KafkaConfig(
    topic = "signal-flow",
    groupId = "flow-signal",
    servers = List("localhost:9092")))

  def run(args: List[String]): IO[ExitCode] = {

   /* flowSignalTopic
      .consume
      .compile
      .drain
      .unsafeRunAsyncAndForget()*/


    val qkJob = jobs(3)

    project(qkJob(0) <-- (qkJob(1) :: qkJob(2) :: dataprepJob)).as(ExitCode.Success)
  }

  private def pushOnce(msg : (String,String), duration : FiniteDuration) =   //(workflowML.workflowId, "trigger-next-stepu")
    fs2.Stream.awakeEvery[IO](duration).head *>
    flowSignalTopic.pushOne(msg)


  private def project(jobs : FlowWorkflow) = FlowProject()(jobs).start()

  private def jobs(howMuch : Int): Vector[Job[FlowScheduling]] = Vector.tabulate(howMuch)(i =>
    Job(i.toString, FlowScheduling())((_: Execution[_]) => Future.successful(Completed))
  )

  private val workflowML = FlowProject(description = "Testing code to implement flow workflow with signal") {
    booJob dependsOn (
      modellingJob dependsOn (
        dataprepJob and makeTrainJob and SignallingJob.kafkaSignaledJob("Enclenche", "trigger-next-stepu", kafkaService = flowSignalTopic)
        ) and (
        fooJob dependsOn makePredictionJob
        )
      )
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
