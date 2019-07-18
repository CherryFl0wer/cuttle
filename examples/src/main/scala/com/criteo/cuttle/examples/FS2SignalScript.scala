package com.criteo.cuttle.examples

import cats.effect._
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.{Finished, Job, Output}
import com.criteo.cuttle.flow.{FlowGraph, FlowScheduling, SchedulerManager}
import com.criteo.cuttle.flow.signals._
import io.circe.Json


object FS2SignalScript extends IOApp {

  import io.circe.syntax._
  import fs2._
  import scala.concurrent.duration._

  val workflow1 : WFSignalBuilder[String, EventSignal] = topic => {
    val job1    = Job(s"step-one",     FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get + " passed to step two"
      IO(Finished).unsafeToFuture()
    }
    val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
      e.streams.info("On step bis")
     /* val receive = for {
        sig   <- topic.sigRef
        value <- topic.subscribeOnTopic(e.context.workflowId)
          .interruptWhen(sig)
          .head
          .compile
          .toList
        _ <- IO(println(s"Received $value"))
      } yield {
        Output(Json.obj("aud" -> "step is bis".asJson))
      }*/
      IO(Output(Json.obj("aud" -> "step is bis".asJson))).unsafeToFuture()
     // receive.unsafeToFuture()
    }
    val job2    = Job(s"step-two",     FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      val x = e.optic.aud.string.getOption(in).get + " passed to step three"
      IO(Output(x.asJson)).unsafeToFuture()
    }
    job1bis --> job2
  }

  def run(args: List[String]): IO[ExitCode] = {

    val kafkaConfig = KafkaConfig("cuttle_message", "cuttlemsg", List("localhost:9092"))
    val program = for {

      signalManager <- Stream.eval(KafkaNotification[String, EventSignal](kafkaConfig))
      scheduler <- SchedulerManager(20)
      workflowWithTopic = workflow1(signalManager)

      newGraph = for {
         wf <- Stream.emit(workflowWithTopic).covary[IO]
         graph  <- Stream.eval(FlowGraph("example-1", "Run jobs with signal")(wf))
         _ <- Stream.eval(scheduler.push(graph))
         _ <- Stream.eval(signalManager.newTopic(graph.workflowId))
      //   _ <- Stream.sleep(4.seconds) ++ signalManager.pushOne(graph.workflowId, SigKillJob("step-one-bis")).map(_ => ())
      } yield graph

      createGraph = Stream.awakeEvery[IO](5.seconds).flatMap(_ => newGraph)
      res <- Stream(
        scheduler.run,
        createGraph
      ).parJoinUnbounded

    } yield res


    program.compile.drain.unsafeRunSync()

    /*
        val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    val cs = IO.contextShift(ec)
    val timerKafka : Timer[IO] = IO.timer(ec)
      */
    //ec.shutdown()

    IO(ExitCode.Success)
  }
}
