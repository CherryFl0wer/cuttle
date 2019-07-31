package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import com.criteo.cuttle.flow.FlowSchedulerUtils.{FlowJob, JobState}
import com.criteo.cuttle.flow.WorkflowsManager.QueueData
import com.criteo.cuttle.{DatabaseConfig, ExecutionPlatform, Executor, Logger, RetryStrategy, XA, platforms, Database => CoreDB}
import com.criteo.cuttle.flow.{Database => FlowDB}

import scala.concurrent.duration.Duration

class FlowCreator(val workflowId: String,
                  val description: String,
                  val jobs: FlowWorkflow,
                  val logger: Logger) {
  /**
    Start the workflow and execute with the given environment.
    Create the transactor to the database, and update database.
    This method is used when you want to run only one workflow at a time.
    * @param dbConfig  The environment of the database
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param retryStrategy The strategy to use for execution retry. Default to None, no retry
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    */
  def start(dbConfig : DatabaseConfig = DatabaseConfig.fromEnv,
            platforms: Seq[ExecutionPlatform] = FlowCreator.defaultPlatforms,
            retryStrategy: Option[RetryStrategy] = None,
            logsRetention: Option[Duration] = None) : fs2.Stream[IO, Either[Throwable, Set[FlowSchedulerUtils.RunJob]]] = {

    import doobie.implicits._
    import fs2.Stream
    import cats.effect.concurrent.Ref

    for {
      xa <- Stream.eval(CoreDB.connect(dbConfig)(logger))
      executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, "version", logsRetention)(retryStrategy)
      _  <- Stream.eval(FlowDB.doSchemaUpdates.transact(xa))
      _ = logger.info("Database up-to-date")
      refState   <- Stream.eval(Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState]))
      scheduler  =  FlowScheduler(logger, workflowId, refState)
      serialize  <- Stream.eval(FlowDB.serializeGraph(jobs).value.transact(xa))
      stream <- serialize match {
        case Left(e) =>
          Stream.eval(logger.error(e.getMessage)).map(_ =>  Left(e))
        case Right(_) =>
          Stream.eval(logger.info(s"Start workflow $workflowId")).flatMap { _ =>
            scheduler.startStream(jobs, executor, xa, logger)
          }
      }
    } yield stream

  }


  /**
    Start scheduling the workflow in a parallel context where multiple workflow can be run at multiple times
    Provide the transactor and a semaphore.
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param retryStrategy The strategy to use for execution retry. Default to None, no retry
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    */
  def parStart(xa : XA,
               semaphore : Semaphore[IO],
               platforms: Seq[ExecutionPlatform] = FlowCreator.defaultPlatforms,
               retryStrategy: Option[RetryStrategy] = None,
               logsRetention: Option[Duration] = None) :
  fs2.Stream[IO, Either[Throwable, Set[FlowSchedulerUtils.RunJob]]] = {

    import doobie.implicits._
    import fs2.Stream
    import cats.effect.concurrent.Ref
    val executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, "version", logsRetention)(retryStrategy)

    for {
      refState   <- Stream.eval(Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState]))
      scheduler  =  FlowScheduler(logger, workflowId, refState)
      _ <- Stream.eval(semaphore.acquire)
      serialize  <- Stream.eval(FlowDB.serializeGraph(jobs).value.transact(xa))
      _ <- Stream.eval(semaphore.release)
      stream <- serialize match {
        case Left(e) =>
          Stream.eval(logger.error(e.getMessage)).map(_ =>  Left(e))
        case Right(_) =>
          Stream.eval(logger.info(s"Start workflow $workflowId")).flatMap { _ =>
            scheduler.startStream(jobs, executor, xa, logger)
          }
      }
    } yield stream

  }
}

object FlowCreator {


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
    * @param description The project description
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(description: String = "", workflowID : String = Instant.now() + "-" + UUID.randomUUID().toString)
           (jobs: FlowWorkflow)
           (implicit logger: Logger) =
    IO(new FlowCreator(workflowID, description, jobs, logger))

}
