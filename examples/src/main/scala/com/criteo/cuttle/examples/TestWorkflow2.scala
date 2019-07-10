package com.criteo.cuttle.examples

import cats.effect._
import com.criteo.cuttle._
import com.criteo.cuttle.flow._
import com.criteo.cuttle.flow.signals.{JobSignalManager, SigKill, WorkflowBuilder}
import io.circe.Json

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object TestWorkflow2 extends IOApp {


  def run(args: List[String]): IO[ExitCode] = {

    import io.circe.syntax._

    implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

    val workflow = for {
      sig <- JobSignalManager(100)
      wf = WorkflowBuilder(sig) { signalManager =>

        val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
          val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get  + " passed to step two"

          val program = for {
            sendSig <- signalManager
              .send((Some("wf_3"), SigKill("step-one")))
              .map(_ => Output(Json.obj("aud" -> x.asJson)))
          } yield sendSig


          program.unsafeToFuture()
        }

        val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
          e.streams.info("On step bis")

          val program = for {
            sendSig <- signalManager.send((Some("wf_3"), SigKill("step-one")))
          } yield sendSig

          program.unsafeRunSync()

          signalManager
            .subscribeTo("wf_3")
            .evalTap(ms => IO(e.streams.info(s"On ${ms}")))
            .filter { case SigKill(job) => job == "step-one" }
            .head
            .evalTap(_ => IO(e.streams.info("Got it")))
            .compile
            .drain
            .map(_ => Output(Json.obj("aud" -> "step is bis".asJson)))
            .unsafeToFuture()
        }

        val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
          val in = e.job.scheduling.inputs
          val y = e.optic.pedo.string.getOption(in).get
          val x = e.optic.aud.string.getOption(in).get  + " passed to step three"

          Future { Output(x.asJson) }
        }
        (job1 && job1bis) --> job2
      }
    } yield wf

    /*
    val projet = for {
      wf <- workflow
      project <- FlowGraph("test03", "Test of jobs I/O")(wf)
      browse <- project.start()
    } yield browse

    projet.compile.toList.unsafeRunSync()*/
    IO(ExitCode.Success)
  }


}
