package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import com.criteo.cuttle.flow.FlowSchedulerUtils.{FlowJob, JobState}
import com.criteo.cuttle.{ExecutionPlatform, Executor, Logger, RetryStrategy, XA, platforms}
import com.criteo.cuttle.flow.{Database => FlowDB}

import scala.concurrent.duration.Duration

class FlowGraph(val workflowId: String,
                val version: String,
                val description: String,
                val jobs: FlowWorkflow,
                val logger: Logger) {
  /**
    Start scheduling the workflow and execute with the given environment.
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param retryStrategy The strategy to use for execution retry. Default to None, no retry
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    */
  def start(xa : XA,
            platforms: Seq[ExecutionPlatform] = FlowGraph.defaultPlatforms,
            retryStrategy: Option[RetryStrategy] = None,
            logsRetention: Option[Duration] = None) : fs2.Stream[IO, Either[Throwable, Set[FlowSchedulerUtils.RunJob]]] = {

    import doobie.implicits._
    import fs2.Stream
    import cats.effect.concurrent.Ref


    val executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, version, logsRetention)(retryStrategy)

    println(s"STARTING ==== > ${workflowId} < ==== STARTING")
    for {
      refState   <- Stream.eval(Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState]))
      scheduler  =  FlowScheduler(logger, workflowId, refState)
      serialize  <- Stream.eval(FlowDB.serializeGraph(jobs).value.transact(xa))
      stream <- serialize match {
        case Left(e) =>
          println(e.getMessage)
          Stream(Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId")
          scheduler.startStream(jobs, executor, xa, logger)
      }
    } yield stream

  }


  /**
    Start scheduling the workflow in a parallel context where multiple workflow can be run at multiple times
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param retryStrategy The strategy to use for execution retry. Default to None, no retry
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    */
  def parStart(xa : XA,
               semaphore : Semaphore[IO],
               platforms: Seq[ExecutionPlatform] = FlowGraph.defaultPlatforms,
               retryStrategy: Option[RetryStrategy] = None,
               logsRetention: Option[Duration] = None) :
  fs2.Stream[IO, Either[Throwable, Set[FlowSchedulerUtils.RunJob]]] = {

    import doobie.implicits._
    import fs2.Stream
    import cats.effect.concurrent.Ref


    val executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, version, logsRetention)(retryStrategy)

    println(s"STARTING ==== > ${workflowId} < ==== STARTING")
    for {
      refState   <- Stream.eval(Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState]))
      scheduler  =  FlowScheduler(logger, workflowId, refState)
      _ <- Stream.eval(semaphore.acquire)
      serialize  <- Stream.eval(FlowDB.serializeGraph(jobs).value.transact(xa))
      _ <- Stream.eval(semaphore.release)
      stream <- serialize match {
        case Left(e) =>
          println(e.getMessage)
          Stream(Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId")
          scheduler.startStream(jobs, executor, xa, logger)
      }
    } yield stream

  }
}

object FlowGraph {


  def defaultPlatforms: Seq[ExecutionPlatform] = {
    import platforms._
    Seq(
      local.LocalPlatform(
        maxForkedProcesses = 5
      )
    )
  }


  /**
    * Create a new graph scheduler to start.
    * @param version The project version as displayed in the UI.
    * @param description The project version as displayed in the UI.
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(version: String = "", description: String = "", workflowID : String = Instant.now() + "-" + UUID.randomUUID().toString)
           (jobs: FlowWorkflow)
           (implicit logger: Logger) =
    IO(new FlowGraph(workflowID, version, description, jobs, logger))

}
