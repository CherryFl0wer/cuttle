package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.criteo.cuttle.flow.FlowSchedulerUtils.{FlowJob, JobState}
import com.criteo.cuttle.{DatabaseConfig, ExecutionPlatform, Executor, Logger, RetryStrategy, platforms, Database => CoreDB}
import com.criteo.cuttle.flow.{Database => FlowDB}
import io.circe.Json

import scala.concurrent.duration.Duration

/**
  * @todo Change it to fit to Cats
* */
class FlowProject(val workflowId: String,
                  val version: String,
                  val description: String,
                  val jobs: FlowWorkflow,
                  val logger: Logger) {

  import cats.implicits._
  /**
    * Start scheduling and execution with the given environment.
    *
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param databaseConfig JDBC configuration for MySQL server 5.7. @TODO : Change db type
    * @param retryStrategy The strategy to use for execution retry. Default to exponential backoff.
    * @param paused Automatically pause all jobs at startup. @unused yet
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    *
    * @param
    */
  def start(
             platforms: Seq[ExecutionPlatform] = FlowProject.defaultPlatforms,
             retryStrategy: Option[RetryStrategy] = None,
             paused: Boolean = false,
             databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
             logsRetention: Option[Duration] = None
           ) : fs2.Stream[IO, Either[Throwable, Set[FlowSchedulerUtils.RunJob]]] = {

    import doobie.implicits._
    import fs2.Stream

    val xa = CoreDB.connect(databaseConfig)(logger)
    val executor = new Executor[FlowScheduling[FlowArg, FlowArg]](platforms, xa, logger, workflowId, version, logsRetention)(retryStrategy)

    logger.info("Applying migrations to database")
    import cats.effect.concurrent.Ref

    for {
      _      <- Stream.eval(FlowDB.doSchemaUpdates.transact(xa))
      _       = logger.info("Database up-to-date")

      refState   <- Stream.eval(Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState]))
      refResults <- Stream.eval(Ref.of[IO, Map[FlowJob, Json]](Map.empty[FlowJob, Json]))

      scheduler = FlowScheduler(logger, workflowId, refState, refResults)
      serialize <- Stream.eval(FlowDB.serializeGraph(jobs).value.transact(xa))
      stream <- serialize match {
        case Left(e) => Stream(Left(e))
        case Right(_) =>
          logger.info("Start workflow")
          scheduler.startStream(jobs, executor, xa, logger)
      }
    } yield stream

  }

}

object FlowProject {


  private[FlowProject] def defaultPlatforms: Seq[ExecutionPlatform] = {
    import platforms._

    Seq(
      local.LocalPlatform(
        maxForkedProcesses = 10
      )
    )
  }

  /**
    * Create a new project.
    * @param workflowId workflow id
    * @param version The project version as displayed in the UI.
    * @param description The project version as displayed in the UI.
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(version: String = "", description: String = "")
           (jobs: FlowWorkflow)
           (implicit logger: Logger): FlowProject = // implicit wfd : Decoder[FlowWorkflow]
    new FlowProject(Instant.now() + "-" + UUID.randomUUID().toString, version, description, jobs, logger)

}
