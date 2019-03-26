package com.criteo.cuttle.flow

import java.util.UUID

import com.criteo.cuttle.{DatabaseConfig, ExecutionPlatform, Executor, Logger, RetryStrategy, platforms, Database => FlowDB}
import io.circe.Decoder

import scala.concurrent.duration.Duration

class FlowProject(val workflowId: String,
                  val version: String,
                  val description: String,
                  val jobs: FlowWorkflow,
                  val logger: Logger) {

  /**
    * Start scheduling and execution with the given environment. It also starts
    * an HTTP server providing an Web UI and a JSON API.
    *
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param databaseConfig JDBC configuration for MySQL server 5.7.
    * @param retryStrategy The strategy to use for execution retry. Default to exponential backoff.
    * @param paused Automatically pause all jobs at startup.
    * @param stateRetention If specified, automatically clean the timeseries state older than the given duration. @unused
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    * @param maxVersionsHistory If specified keep only the version information for the x latest versions. @unused
    */
  def start(
             platforms: Seq[ExecutionPlatform] = FlowProject.defaultPlatforms,
             databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
             retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy,
             paused: Boolean = false,
             stateRetention: Option[Duration] = None,
             logsRetention: Option[Duration] = None,
             maxVersionsHistory: Option[Int] = None
           ): Unit = {
    val startScheduler = build(platforms, databaseConfig, retryStrategy, paused, logsRetention)

    logger.info(s"Running cuttle flow graph")

    startScheduler()
  }

  /**
    * Connect to database and decide when to start the scheduling.
    *
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param databaseConfig JDBC configuration for MySQL server 5.7.
    * @param retryStrategy The strategy to use for execution retry. Default to exponential backoff.
    * @param paused Automatically pause all jobs at startup.
    * @param stateRetention If specified, automatically clean the timeseries state older than the given duration.
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration.
    * @param maxVersionsHistory If specified keep only the version information for the x latest versions.
    *
    * @return """a tuple with cuttleRoutes (needed to start a server)""" and a function to start the scheduler
    *      -> TODO : might delete TimeSeriesApp later
    */
  def build(
             platforms: Seq[ExecutionPlatform] = FlowProject.defaultPlatforms,
             databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
             retryStrategy: RetryStrategy = RetryStrategy.ExponentialBackoffRetryStrategy,
             paused: Boolean = false,
             logsRetention: Option[Duration] = None
           ):  () => Unit = {
    val xa = FlowDB.connect(databaseConfig)(logger)
    val executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, version, logsRetention)(retryStrategy)
    val scheduler = new FlowScheduler(logger, workflowId)

    val startScheduler = () => {
      // Pause strategy here
      // ....

      logger.info("Start workflow")
      scheduler.start(jobs, executor, xa, logger)
    }

    startScheduler
  }
}

object FlowProject {

  /**
    * Create a new project.
    * @param workflowId workflow id
    * @param version The project version as displayed in the UI.
    * @param description The project version as displayed in the UI.
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(workflowId: String = "workflow-" + UUID.randomUUID().toString, version: String = "", description: String = "")
           (jobs: FlowWorkflow)(implicit logger: Logger): FlowProject = // wfd : Decoder[FlowWorkflow]
    new FlowProject(workflowId, version, description, jobs, logger)


  private[FlowProject] def defaultPlatforms: Seq[ExecutionPlatform] = {
    import platforms._

    Seq(
      local.LocalPlatform(
        maxForkedProcesses = 10
      )
    )
  }
}