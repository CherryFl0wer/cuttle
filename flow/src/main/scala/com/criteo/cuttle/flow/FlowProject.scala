package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import com.criteo.cuttle.{DatabaseConfig, ExecutionPlatform, Executor, Logger, RetryStrategy, platforms, Database => FlowDB}

import scala.concurrent.duration.Duration

/**
  * @todo Change it to fit to Cats
* */
class FlowProject(val workflowId: String,
                  val version: String,
                  val description: String,
                  val jobs: FlowWorkflow,
                  val logger: Logger) {

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

    val xa = FlowDB.connect(databaseConfig)(logger)
    val executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, version, logsRetention)(retryStrategy)
    val scheduler = FlowScheduler(logger, workflowId)

    logger.info("Start workflow")
    scheduler.startStream(jobs, executor, xa, logger)
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
