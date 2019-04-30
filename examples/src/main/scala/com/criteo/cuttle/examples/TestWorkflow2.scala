package com.criteo.cuttle.examples

import cats.effect._
import cats.implicits._
import com.criteo.cuttle._
import com.criteo.cuttle.flow._
import com.criteo.cuttle.flow.utils.JobUtils

import scala.concurrent.Future


/**
  * The goal of this test is to get to work the routing system
  */

object TestWorkflow2 extends IOApp {

  import com.criteo.cuttle.platforms.local._
  import io.circe.Json
  import io.circe.parser.parse
  import io.circe.syntax._

  def run(args: List[String]): IO[ExitCode] = {
    // Normally job are idempotent you can have one for all
/*
    // Fail part3 (part1 && part2 successAndError(S, E)) --> S get cycle
    val part1 = dataprepJob.successAndError(booJob, errorJob) && fooJob.successAndError(booJob, error2Job)
    val part2 = makeTrainJob --> makePredictionJob("Step-Training")
    val part3 = (part1 --> modellingJob && (part2 successAndError(modellingJob, error3Job)))
    val wf = part3 successAndError(endJob, error4Job)*/

    val wf = dataprepJob successAndError(booJob, errorJob)
    val run = FlowProject()(wf).start[IO]()
    val list = run.compile.toList.unsafeRunSync
    list.foreach(p => println(p))

    IO(ExitCode.Success)
  }

  private def jobs(howMuch : Int): Vector[Job[FlowScheduling]] = Vector.tabulate(howMuch)(i =>
    Job(i.toString, FlowScheduling())((_: Execution[_]) => Future.successful(Finished))
  )

  private val errorJob = {
    Job("Step-Error", FlowScheduling(), "Error") {
      implicit e =>

        e.streams.info("We got an error :'( ")
        Future.successful(Finished)
    }
  }

  private val error2Job = {
    Job("Step-Error2", FlowScheduling(), "Error") {
      implicit e =>

        e.streams.info("We got an error 2 :'( ")
        Future.successful(Finished)
    }
  }

  private val error3Job = {
    Job("Step-Error3", FlowScheduling(), "Error") {
      implicit e =>

        e.streams.info("We got an error 3 :'( ")
        Future { Finished }
    }
  }

  private val error4Job = {
    Job("Step-Error4", FlowScheduling(), "Error") {
      implicit e =>

        e.streams.info("We got an error 4 :'( ")
        Future { Finished }
    }
  }

  private val endJob = {
    Job("Step-End", FlowScheduling(), "Ending process") {
      implicit e =>

        e.streams.info("We got an End  :) ")

        Future.successful(Finished)
    }
  }

  private val booJob = {
    Job("Step-Boo", FlowScheduling(Some("{param: \"ok\"}")), "Booing") {
      implicit e =>

        e.streams.info("Testing Boo")
        val jsonInputs : Json = parse(e.job.scheduling.inputs.get) match {
          case Left(_) => Json.Null
          case Right(parsed) => parsed
        }

        e.context.result = Json.obj(
          "response" -> "as two".asJson,
          "inputsWas" -> jsonInputs
        )

        Future.successful(Finished)
    }
  }

  private val fooJob = {
    Job("Step-Foo", FlowScheduling(), "Fooing") {
      implicit e: Execution[FlowScheduling] =>
        e.streams.info("Testing Foo")
        Future.successful(Finished)
    }
  }

  private val modellingJob = {
    Job("Step-Modelling", FlowScheduling(), "modeling") {
      implicit e =>
        e.streams.info("Testing Modelling")
        Future.successful(Finished)
    }
  }

  private val makePredictionJob = (prevStep : String) => {
    Job("Step-MakePredic", FlowScheduling(), "predicting") {
      implicit e =>
        e.streams.info("Testing MakePredic")
        e.streams.writeln(e.context.resultsFromPreviousNodes.get(prevStep).toString)
        Future.successful(Finished)
    }
  }

  private val makeTrainJob = {
    Job("Step-Training", FlowScheduling(), "train") {
      implicit e =>
        e.streams.info("Testing Training")
        e.context.result = Json.obj(
          "name" -> "job training".asJson
        )
        Future.successful(Finished)
    }
  }

  private val dataprepJob = {
    Job("Step-Dataprep", FlowScheduling(), "Data preparation") {
      implicit e =>

        val waitShell = exec"""sh -c '
         |    echo Looping for 5 seconds...
         |    for i in `seq 1 5`
         |    do
         |        date
         |        sleep 1
         |    done
         |    echo Ok
         |'""" ()

        waitShell.flatMap(_ => JobUtils.failedJob("Job error failed"))
    }
  }
}
