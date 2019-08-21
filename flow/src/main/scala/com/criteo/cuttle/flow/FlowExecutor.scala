package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.{Concurrent, IO}
import cats.effect.concurrent.{Ref, Semaphore}
import com.criteo.cuttle.flow.FlowSchedulerUtils.{FlowJob, JobState}
import com.criteo.cuttle.{ExecutionPlatform, Executor, Logger, XA, platforms}
import com.criteo.cuttle.flow.{Database => FlowDB}
import cats.implicits._
import scala.concurrent.duration.Duration

/**
  * @param xa XA transactor
  * @param logRetention If specified, automatically clean the execution logs older than the given duration. @unused
  * @param workflowId The unique id of the workflow
  * @param description A description given to the workflow
  * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
  * @param jobs The way jobs are handled
  * @param logger To print message in STDOUT
  */
class FlowExecutor(xa : XA,
                   executor : Executor[FlowScheduling],
                   logRetention : Option[Duration],
                   platforms: Seq[ExecutionPlatform],
                   val workflowId: String,
                   val description: String,
                   val jobs: FlowWorkflow,
                   val workflowRef : Ref[IO, FlowWorkflow],
                   val stateRef : Ref[IO, JobState])(implicit C: Concurrent[IO], logger: Logger) {
  import doobie.implicits._
  import cats.effect.concurrent.Ref


  /**
    Start the workflow and execute with the given environment.
    Create the transactor to the database and serialize the graph if it does not exist.
    This method is used when you want to run only one workflow at a time.
    */
  def start: IO[Either[Throwable, (FlowWorkflow, JobState)]] =
    for {
      scheduler  <-  IO.pure(FlowScheduler(logger, workflowId, stateRef, workflowRef, jobs.hash))
      serialize  <- FlowDB.serializeGraph(jobs).value.transact(xa)
      done <- serialize match {
        case Left(e) =>
          logger.error(e.getMessage).map(_ =>  Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId") *>
            scheduler.executeWorkflow(executor, xa, logger).value
      }
    } yield done

  /**
    Start scheduling the workflow in a parallel context where multiple workflow can be run at multiple times
    Provide the transactor and a semaphore.
    * @param semaphore Semaphore to avoid serializing the first time the graph is seen
    *                  ex : If two workflow depends on the same FlowWorkflow and have never been executed before
    *                       then the serialization in parallel could try to insert the new FlowWorkflow two times
    */
  def parStart(semaphore : Semaphore[IO]): IO[Either[Throwable, (FlowWorkflow, JobState)]] =
    for {
      scheduler  <-  IO.pure(FlowScheduler(logger, workflowId, stateRef, workflowRef, jobs.hash))
      _ <- semaphore.acquire
      serialize  <- FlowDB.serializeGraph(jobs).value.transact(xa)
      _ <- semaphore.release
      done <- serialize match {
        case Left(e) =>
          logger.error(e.getMessage).map(_ =>  Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId") *>
            scheduler.executeWorkflow(executor, xa, logger).value
      }

    } yield done



  /**
    Run a job from the workflow
    * @param jobId the id of the job to run in the workflow
    */
  def runSingleJob(jobId: String): IO[Either[Throwable, (FlowWorkflow, JobState)]] = {

    val job = jobs.vertices.find(j => j.id == jobId)
    if (job.isEmpty) IO.pure(Left(new Throwable(s"$jobId does not exist")))
    else for {
      scheduler   <- IO.pure(FlowScheduler(logger, workflowId, stateRef, workflowRef, jobs.hash))
      serialize   <- FlowDB.serializeGraph(jobs).value.transact(xa)
      tmpSave <- scheduler.refWorkflow.get
      done <- serialize match {
        case Left(e) =>
          logger.error(e.getMessage).map(_ =>  Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId with job ${job.get.id}") *>
            scheduler.refWorkflow.set(job.get) *>
            scheduler.executeWorkflow(executor, xa, logger).value
      }
      _ <- scheduler.refWorkflow.set(tmpSave)
    } yield done
  }


}

object FlowExecutor {
  def defaultPlatforms: Seq[ExecutionPlatform] = {
    import platforms._
    Seq(
      local.LocalPlatform(5)
    )
  }

  /**
    * Prepare the workflow to be started
    * @param xa transactor SQL
    * @param description The project description
    * @param workflowID id given to the workflow
    * @param platforms define the resources that will be used by the executor
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug information.
    */
  def apply(xa : XA,
            description: String = "",
            workflowID : String = Instant.now() + "-" + UUID.randomUUID().toString,
            platforms: Seq[ExecutionPlatform] = defaultPlatforms,
            logsRetention : Option[Duration] = None)
           (jobs: FlowWorkflow)
           (implicit logger: Logger, C : Concurrent[IO]): IO[FlowExecutor] = for {
    wf <- Ref.of[IO, FlowWorkflow](jobs)
    state <- Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState])
    executor <- IO.pure(new Executor[FlowScheduling](platforms, xa, logger, workflowID, logsRetention)(None))
    flow     <- IO.pure(new FlowExecutor(xa, executor, logsRetention, platforms, workflowID, description, jobs, wf, state))
  } yield flow

}
