package com.criteo.cuttle.examples

import cats.effect._
import com.criteo.cuttle._
import com.criteo.cuttle.flow._
import io.circe.Json
import scala.concurrent.Future


object TestWorkflow2 extends IOApp {

  import com.criteo.cuttle.platforms.local._

  def run(args: List[String]): IO[ExitCode] = {

    import io.circe.syntax._

    val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get  + " passed to step two"
      Future.successful(Output(Json.obj("aud" -> x.asJson)))
    }

    val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
      Future.successful(Output(Json.obj("aud" -> "albuquerque".asJson)))
    }

    val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("pedo" -> "velo".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      val y = e.optic.pedo.string.getOption(in)
      val x = e.optic.aud.string.getOption(in)  + " passed to step three"
      Future.successful(Output(x.asJson))
    }

    val wf = (job1 && job1bis) --> job2

    val project = FlowProject("test03", "Test of jobs I/O")(wf)
    val browse = project.start().compile.toList.unsafeRunSync

    browse.foreach(p => println(p))
    IO(ExitCode.Success)
  }

  private val errorJob = {
    Job("Step-Error", FlowScheduling(), "Error") {
      implicit e =>

        e.streams.info("We got an error :'( ")
        Future { Finished }
    }
  }
  private val fooJob = Job("Step-Foo", FlowScheduling(), "Fooing") {
    implicit e: Execution[FlowScheduling] =>
      import io.circe.syntax._
      e.streams.info("Testing Foo")

      Future { Output( Json.obj("testing" -> "valid".asJson)) }
  }

  private val endJob = Job("Step-End", FlowScheduling(), "Ending process") {
      implicit e: Execution[FlowScheduling] =>
        e.streams.info("We got to an End  :) ")
        val testing = e.optic.testing.string.getOption(e.job.scheduling.inputs)
        e.streams.info(testing.fold("Nothing")(identity))
        e.streams.info()
        Future.successful(Finished)
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

        waitShell //.flatMap(_ => JobUtils.failedJob("Job error failed"))
    }
  }
}
