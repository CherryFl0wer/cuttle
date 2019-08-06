package com.criteo.cuttle.examples

import cats.effect._
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.{DatabaseConfig, Finished, Job, Output, OutputErr}
import com.criteo.cuttle.flow.{FlowCreator, FlowScheduling, WorkflowsManager}
import com.criteo.cuttle.flow.signals._
import com.criteo.cuttle.flow.utils.KafkaConfig
import io.circe.Json


object FS2SignalScript extends IOApp {

  import io.circe.syntax._
  import fs2._

  val workflow1 : WFSignalBuilder[String, EventSignal] = topic => {
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
          .toList
        _ <- IO(println(s"Received $value"))
      } yield {
        OutputErr(Json.obj("err" -> "Oh no uwu".asJson))
      }
      receive.unsafeToFuture()
    }

    val job2    = Job(s"step-two",  FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      val x = e.optic.result.string.getOption(in).get + " got it"
      IO(Output(Json.obj("exploded" -> x.asJson))).unsafeToFuture()
    }
    job1.error(errJob) --> job2
  }

  import com.criteo.cuttle.{ Database => CoreDB }
  def run(args: List[String]): IO[ExitCode] = {

    val kafkaConfig = KafkaConfig("cuttle_message", "cuttlemsg", List("localhost:9092"))
    /*
    This testing program is used to run multiple workflow at the same time.
    By setting up a workflow manager that take the length of the queue and a signalManager
    */
    /*
    val program = for {
      signalManager <- Stream.eval(SignalManager[String, EventSignal](kafkaConfig))
      scheduler     <- WorkflowsManager(20)(signalManager)
      workflowWithTopic = workflow1(signalManager)

      generateGraphs = for {
         graph <- Stream(()).repeat.take(6).evalMap(_ => FlowCreator("example", "Run jobs with signal")(workflowWithTopic))
         _     <- Stream.eval(scheduler.push(graph))
      } yield graph

      eagerList       <- Stream.eval(generateGraphs.compile.toList)
      graphList        = Stream.emits(eagerList).covary[IO]
      workflowListId   = Stream.emits(eagerList.map(g => g.workflowId)).covary[IO]
      _ = logger.info("Running these workflow : ")
      _ <- graphList.take(6).map(x => println(s"${x.workflowId} launching..."))

      res <- Stream(
         scheduler.run(2).drain,
         signalManager.broadcastTopic,
         graphList.drain,
         Stream.awakeEvery[IO](1.second).zipRight(workflowListId.repeat).flatMap { g =>
           signalManager.pushOne(g, SigKillJob("step-one-bis"))
         }.drain
      ).parJoinUnbounded
    } yield res

    program.compile.drain.unsafeRunSync() */

    val programInitiate = for {
      // Initialisation
      xa <- Stream.eval(CoreDB.connect(DatabaseConfig.fromEnv)(logger))
      signalManager <- Stream.eval(SignalManager[String, EventSignal](kafkaConfig))
      scheduler     <- WorkflowsManager(xa, signalManager)()
      workflowWithTopic = workflow1(signalManager)

      // First run
      graph  <- Stream.eval(FlowCreator(xa, "Run jobs with signal")(workflowWithTopic))
      fstRes <- Stream.eval(scheduler.runOne(graph))

      // Second run
      //sndRes <- Stream.eval(scheduler.runSingleJob(graph, "step-two"))
    } yield fstRes

    val resultat = programInitiate.compile.toList.unsafeRunSync()


    IO(ExitCode.Success)
  }
}
