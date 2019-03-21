package com.criteo.cuttle.flow

import com.criteo.cuttle.{DatabaseConfig, Database => FlowDB, ExecutionPlatform, Executor, Logger, RetryStrategy, platforms}
import io.circe.Decoder

import scala.concurrent.duration.Duration

class FlowProject(val name: String,
                  val version: String,
                  val description: String,
                  val env: (String, Boolean),
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
    * @param stateRetention If specified, automatically clean the timeseries state older than the given duration.
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration.
    * @param maxVersionsHistory If specified keep only the version information for the x latest versions.
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
    val executor = new Executor[FlowScheduling](platforms, xa, logger, name, version, logsRetention)(retryStrategy)
    val scheduler = new FlowScheduler(logger)

    val startScheduler = () => {
      logger.info("Start workflow")
      scheduler.start(jobs, executor, xa, logger)
    }

    startScheduler
  }
}

object FlowProject {

  /**
    * Create a new project.
    * @param name The project name as displayed in the UI.
    * @param version The project version as displayed in the UI.
    * @param description The project version as displayed in the UI.
    * @param env The environment as displayed in the UI (The string is the name while the boolean indicates
    *            if the environment is a production one).
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(name: String,
            version: String = "",
            description: String = "",
            env: (String, Boolean) = ("", false)
           )(jobs: FlowWorkflow)(implicit logger: Logger): FlowProject = // wfd : Decoder[FlowWorkflow]
    new FlowProject(name, version, description, env, jobs, logger)


  private[FlowProject] def defaultPlatforms: Seq[ExecutionPlatform] = {
    import platforms._

    Seq(
      local.LocalPlatform(
        maxForkedProcesses = 10
      )
    )
  }
}
