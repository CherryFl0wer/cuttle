package com.criteo.cuttle.flow

import java.time.Instant
import java.util.UUID

import cats.effect.{Concurrent, IO}
import cats.effect.concurrent.Semaphore
import com.criteo.cuttle.flow.FlowSchedulerUtils.{FlowJob, JobState}
import com.criteo.cuttle.{ExecutionPlatform, Executor, Logger, XA, platforms}
import com.criteo.cuttle.flow.{Database => FlowDB}
import io.circe.Json
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
class FlowCreator(xa : XA,
                  executor : Executor[FlowScheduling],
                  logRetention : Option[Duration],
                  val workflowId: String,
                  val description: String,
                  platforms: Seq[ExecutionPlatform],
                  val jobs: FlowWorkflow,
                  logger: Logger)(implicit C: Concurrent[IO]) {
  import doobie.implicits._
  import fs2.concurrent.SignallingRef
  import cats.effect.concurrent.Ref

  val workflowIsOver: IO[SignallingRef[IO, Boolean]] = SignallingRef[IO, Boolean](true)


  /**
    Start the workflow and execute with the given environment.
    Create the transactor to the database, and update database.
    This method is used when you want to run only one workflow at a time.
    */
  def start: IO[Either[Throwable, (FlowWorkflow, JobState)]] =
    for {
      sigOver <- workflowIsOver
      _ <- sigOver.set(false)
      refState   <- Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState])
      scheduler  =  FlowScheduler(logger, workflowId, refState)
      serialize  <- FlowDB.serializeGraph(jobs).value.transact(xa)
      done <- serialize match {
        case Left(e) =>
          logger.error(e.getMessage).map(_ =>  Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId").flatMap { _ =>
            scheduler.executeWorkload(jobs, executor, xa, logger).value
          }
      }
      _ <- sigOver.set(true)
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
      sigOver <- workflowIsOver
      _ <- sigOver.set(false)
      refState   <- Ref.of[IO, JobState](Map.empty[FlowJob, JobFlowState])
      scheduler  =  FlowScheduler(logger, workflowId, refState)
      _ <- semaphore.acquire
      serialize  <- FlowDB.serializeGraph(jobs).value.transact(xa)
      _ <- semaphore.release
      done <- serialize match {
        case Left(e) =>
          logger.error(e.getMessage).map(_ =>  Left(e))
        case Right(_) =>
          logger.info(s"Start workflow $workflowId").flatMap { _ =>
            scheduler.executeWorkload(jobs, executor, xa, logger).value
          }
      }
      _ <- sigOver.set(true)
    } yield done


  /**
    * @param jobId Id given to the job in the [[FlowWorkflow]]
    * @param input If is not None then will replace the json input of the job by the new input otherwise take
    *              parent's input
    */
  def runSingleJob(jobId: String, input : Option[Json] = None)  = {

    val job = jobs.vertices.filter(j => j.id == jobId).head

    val newInput = if (input.isEmpty) for {
      lstOfParent <- FlowDB.retrieveWorkflowResults(workflowId).transact(xa)
      parents = jobs.parentsFromRoute(job, RoutingKind.Success).map(_.id).toList
      parentsJsonOutput = lstOfParent.filter(j => parents.contains(j._1))
      newJobInput = FlowSchedulerUtils.mergeDuplicateJson((job.id, Json.Null), parentsJsonOutput)
    } yield newJobInput._2 else IO.pure(input.get)

    newInput.map(inp => jobAsWorkflow(job.copy(scheduling = FlowScheduling(inputs = inp))(job.effect)))
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
    * Prepare the workflow to be started
    * @param xa transactor SQL
    * @param description The project description
    * @param workflowID id given to the workflow
    * @param platforms define the resources that will be used by the executor
    * @param jobs The workflow to run in this project.
    * @param logger The logger to use to log internal debug informations.
    */
  def apply(xa : XA,
            description: String = "",
            workflowID : String = Instant.now() + "-" + UUID.randomUUID().toString,
            platforms: Seq[ExecutionPlatform] = defaultPlatforms,
            logsRetention : Option[Duration] = None)
           (jobs: FlowWorkflow)
           (implicit logger: Logger, C : Concurrent[IO]): IO[FlowCreator] = for {
    executor <- IO.pure(new Executor[FlowScheduling](platforms, xa, logger, workflowID, logsRetention)(None))
    flow     <- IO.pure(new FlowCreator(xa, executor, logsRetention, workflowID, description, platforms, jobs, logger))
  } yield flow

}
