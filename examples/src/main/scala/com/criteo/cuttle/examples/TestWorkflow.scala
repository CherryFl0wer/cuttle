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


    val qkJob = jobs(7)

    val wf = (((dataprepJob && fooJob) --> booJob) && (makeTrainJob --> makePredictionJob("Step-Training"))) --> (modellingJob)

    val run = FlowProject()(wf).start[IO]()
    val list = run.compile.toList.unsafeRunSync
    list.foreach(p => println(p))

    IO(ExitCode.Success)
  }

  //@msg = (workflowML.workflowId, "trigger-next-stepu")
  private def pushOnce(msg : (String, String), duration : FiniteDuration) =
    fs2.Stream.awakeEvery[IO](duration).head *>
    flowSignalTopic.pushOne(msg)


  private def jobs(howMuch : Int): Vector[Job[FlowScheduling]] = Vector.tabulate(howMuch)(i =>
    Job(i.toString, FlowScheduling())((_: Execution[_]) => Future.successful(Completed))
  )

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

        Future.successful(Completed)
    }
  }

  private val fooJob = {
    Job("Step-Foo", FlowScheduling(), "Fooing") {
      implicit e: Execution[FlowScheduling] =>
        e.streams.info("Testing Foo")
        Future.successful(Completed)
    }
  }

  private val modellingJob = {
    Job("Step-Modelling", FlowScheduling(), "modeling") {
      implicit e =>
        e.streams.info("Testing Modelling")
        Future.successful(Completed)
    }
  }

  private val makePredictionJob = (prevStep : String) => {
    Job("Step-MakePredic", FlowScheduling(), "predicting") {
      implicit e =>
        e.streams.info("Testing MakePredic")
        e.streams.writeln(e.context.resultsFromPreviousNodes.get(prevStep).toString)
        Future.successful(Completed)
    }
  }

  private val makeTrainJob = {
    Job("Step-Training", FlowScheduling(), "train") {
      implicit e =>
        e.streams.info("Testing Training")
        e.context.result = Json.obj(
          "name" -> "job training".asJson
        )
        Future.successful(Completed)
    }
  }

  private val dataprepJob = {
    Job("Step-Dataprep", FlowScheduling(), "Data preparation") {
      implicit e =>
        exec"""sh -c '
         |    echo Looping for 5 seconds...
         |    for i in `seq 1 5`
         |    do
         |        date
         |        sleep 1
         |    done
         |    echo Ok
         |'""" ()

    }
  }
}
