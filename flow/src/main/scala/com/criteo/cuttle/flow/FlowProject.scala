package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.{Concurrent, IO, Sync}
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import com.criteo.cuttle.{Completed, DatabaseConfig, ExecutionPlatform, Executor, Logger, RetryStrategy, Scheduling, platforms, Database => FlowDB}

import scala.concurrent.Future
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
    * @param paused Automatically pause all jobs at startup.
    * @param stateRetention If specified, automatically clean the timeseries state older than the given duration. @unused
    * @param logsRetention If specified, automatically clean the execution logs older than the given duration. @unused
    * @param maxVersionsHistory If specified keep only the version information for the x latest versions. @unused
    */
  def start[F[_]: Sync](
             platforms: Seq[ExecutionPlatform] = FlowProject.defaultPlatforms,
             retryStrategy: Option[RetryStrategy] = None,
             paused: Boolean = false,
             databaseConfig: DatabaseConfig = DatabaseConfig.fromEnv,
             stateRetention: Option[Duration] = None,
             logsRetention: Option[Duration] = None,
             maxVersionsHistory: Option[Int] = None
           )(implicit C : Concurrent[F]): fs2.Stream[F, Set[(FlowJob, FlowSchedulerContext, Future[Completed])]] = {

    val xa = FlowDB.connect(databaseConfig)(logger)
    val executor = new Executor[FlowScheduling](platforms, xa, logger, workflowId, version, logsRetention)(retryStrategy)
    val scheduler = FlowScheduler(logger, workflowId)

      /*if (paused) {
        logger.info("Pausing workflow")
        scheduler.pauseJobs(jobs.all, executor, xa)
      }*/
    logger.info("Start workflow")
    scheduler.start[F](jobs, executor, xa, logger)
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
