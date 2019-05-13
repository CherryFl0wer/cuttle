package com.criteo.cuttle.flow

import java.time.Instant

import cats.Monad
import cats.data.EitherT
import cats.effect.{Concurrent, IO, Sync}
import cats.implicits._
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle._
import doobie.implicits._
import io.circe._
import io.circe.syntax._

import scala.concurrent.Future
import scala.concurrent.stm._
import scala.collection.mutable.LinkedHashSet


/** A [[FlowScheduler]] executes the [[com.criteo.cuttle.flow.FlowWorkflow Workflow]]
  */
case class FlowScheduler(logger: Logger, workflowdId : String) extends Scheduler[FlowScheduling] {

  import FlowSchedulerUtils._

  override val name = "flow"

  private val _state = Ref(Map.empty[FlowJob, JobFlowState]) // State of a job

  private[flow] def state: State = atomic { implicit txn => _state() }

  private val _results = Ref(Map.empty[FlowJob, Json])

  private[flow] def results: Map[FlowJob, Json] = atomic { implicit txn => _results() }

  private val _pausedJobs = Ref(Set.empty[PausedJob])

  def pausedJobs(): Set[PausedJob] = atomic { implicit txn =>
    _pausedJobs()
  }

  private val queries = Queries(logger)

  private val discardedJob = new LinkedHashSet[FlowJob]()

  private def currentJobsRunning(state : State) : Set[FlowJob] = atomic { implicit txn =>
    state.filter { case (_, jobState) =>
      jobState match {
        case Running(_) => true
        case _ => false
      }
    }.keySet
  }
  private def currentJobsDone(state : State) : Set[FlowJob] = atomic { implicit txn =>
    state.filter { case (_, jobState) =>
      jobState match {
        case Done(_) => true
        case _ => false
      }
    }.keySet
  }
  private def currentJobsFailed(state : State) : Set[FlowJob] = atomic { implicit txn =>
    state.filter { case (_, jobState) =>
      jobState match {
        case Failed(_) => true
        case _ => false
      }
    }.keySet
  }



  /**
    * @param job the job to save
    * @param context the context of the job
    * @param xa doobie sql
    * @summary Save job's result in the database and in a map inmemory.
    *         the map is here to avoid seeking for the result in the db every time we need it
    * */
  private def saveResult(job : FlowJob, context : FlowSchedulerContext, xa : XA) = {
    Database.insertResult(workflowdId, job.id,  job.scheduling.inputs.asJson, context.result)
      .transact(xa)
      .unsafeRunSync()

    atomic { implicit txn =>
      _results() = _results() + (job -> context.result)
    }
  }

  /***
    *
    * @param running jobs set
    * @return tuple of completed and still running job
    */
  private def completion(running : Set[RunJob]) =  running.partition {
    case (_, _, effect) => effect.isCompleted
  }



  private[flow] def initialize(wf : Workload[FlowScheduling], xa : XA, logger : Logger) = {
    val workflow = wf.asInstanceOf[FlowWorkflow]

    logger.info("Validate flow workflow before start")
/*
    val fll = for {
      err <- FlowSchedulerUtils.validate(workflow)
      _ = logger.info("Flow Workflow is valid")
      _ = logger.info("Applying migrations to database")
      _ <- Database.doSchemaUpdates.transact(xa).attempt
      _ = logger.info("Database up-to-date")
      deserializeState <- Database.deserializeState(workflowdId)(workflow.vertices).transact(xa).attempt
      _ = logger.info("Update state")
      state <- deserializeState
      st <- state
      _ = atomic { implicit txn => _state() = st }
      pausedJobsSeq <-  queries.getPausedJobs.transact(xa)
      _ = atomic { implicit txn => _pausedJobs() = _pausedJobs() ++ pausedJobsSeq }
    } yield err

    fll match {
      case Left(errors) =>
        val consolidatedError = errors.mkString("\n")
        logger.error(consolidatedError)
        throw new IllegalArgumentException(consolidatedError)
      case _ => workflow
    } */

    FlowSchedulerUtils.validate(workflow) match {
      case Left(errors) =>
        val consolidatedError = errors.mkString("\n")
        logger.error(consolidatedError)
        throw new IllegalArgumentException(consolidatedError)
      case Right(_) => ()
    }

    logger.info("Flow Workflow is valid")

    logger.info("Applying migrations to database")
    Database.doSchemaUpdates.transact(xa).unsafeRunSync
    logger.info("Database up-to-date")

    logger.info("Update state")
    Database
      .deserializeState(workflowdId)(workflow.vertices)
      .transact(xa)
      .unsafeRunSync
      .foreach {
        state => atomic {
          implicit txn => _state() = state
        }
      }

    atomic { implicit txn =>
      _pausedJobs() = _pausedJobs() ++ queries.getPausedJobs.transact(xa).unsafeRunSync()
    }

    workflow
  }


  /***
    * @summary Select the jobs that will run
    * @param workflow To get the next jobs to run and the result from previous node
    * @param executor To get data for context job
    * @param state Current state of the jobs
    * @return A sequence of executable jobs
    */

  private[flow] def jobsToRun(workflow: FlowWorkflow,
                              executor: Executor[FlowScheduling],
                              newState : State): Seq[Executable] = {

    // Are those which are not running
    def jobsAllowedToRun(nextJobs : Set[FlowJob]) =
       nextJobs
        .diff(currentJobsRunning(newState))
        .foldLeft(Set.empty[FlowJob]) { (acc, job) =>
          if (currentJobsFailed(newState).contains(job)) { // if the jobs has failed then we give its error path for next jobs
            workflow.pathFromVertice(job, RoutingKind.Success).foreach(discardedJob.add)
            discardedJob.add(job)

            val errorChild = workflow.childsFromRoute(job, RoutingKind.Failure)
            errorChild.isEmpty match {
              case true => acc // TODO do something if there is an error in a job but no job to catch it
              case _ => acc ++ errorChild
            }
          }
          else
            acc + job // Normal success job
        }


    // Next jobs ? take off jobs done, discarded ones and error job
    // Error job will be added by jobsAllowedToRun
    val newWorkflow = FlowWorkflow.without(workflow, currentJobsDone(newState) ++ discardedJob.toSet ++ workflow.childFrom(RoutingKind.Failure))
    val roots = newWorkflow.roots
    val toRun = jobsAllowedToRun(roots).map { j =>
      val parentOfJob = workflow.parentsOf(j) // Previous job
      val resultsFromParent = parentOfJob.foldLeft(Map.empty[String, Json])((acc, job) => atomic {
        implicit txn => acc + (job.id -> _results().getOrElse(job, Json.Null))
      })

      (j, FlowSchedulerContext(Instant.now, executor.projectVersion, workflowdId, resultsFromParent))
    }

    toRun.toSeq
  }



  /**
    * @param workflow Workflow used to get the next jobs to run
    * @param executor Execute the side effect of a job
    * @param xa doobie sql
    * @param running Set of current job running (can have completed jobs)
    * @summary Run the jobs and update state of the scheduler
    * */
  private[flow] def runJobs(workflow: FlowWorkflow,
                            executor: Executor[FlowScheduling],
                            xa : XA, running : Set[RunJob]) : IO[Either[Throwable, Set[RunJob]]] = {


    val (completed, stillRunning) = completion(running)

    // Update state and get the next jobs to run
    val (stateSnapshot, toRun) = atomic { implicit txn =>

      def isDone(state: State, job: FlowJob): Boolean = state(job) match  {
        case Done(_) => true
        case _       => false
      }

      // update state with job statuses
      val newState = completed.foldLeft(_state()) {
        case (acc, (job, context, future)) =>

          if (future.value.get.isSuccess || isDone(_state(), job)) {
            saveResult(job, context, xa)
            acc + (job -> Done(context.projectVersion))
          }
          else if(future.value.get.isFailure) {
            saveResult(job, context, xa)
            acc + (job -> Failed(context.projectVersion))
          }
          else acc
      }

      val toRun = jobsToRun(workflow, executor, newState)

      _state() = newState

      (newState, toRun)
    }

    val newExecutions = executor.runAll(toRun) // TODO Change core to return IO execution

    atomic { implicit txn =>
      _state() = newExecutions.foldLeft(_state()) {
        case (st, (execution, _)) =>
          st + (execution.job -> Running(execution.id))
      }
    }

    val statusJobs = stillRunning ++ newExecutions.map {
      case (execution, result) =>
        (execution.job, execution.context, result)
    }

    if(completed.nonEmpty || toRun.nonEmpty) {
      val serializeState = for {
        ei <- EitherT(Database.serializeState(workflowdId, stateSnapshot, None).transact(xa).attempt)
        res = statusJobs
      } yield res
      serializeState.value
    } else IO(Either.right(statusJobs))
  }


  /***
    * Starts the scheduler for the given Workflow. Immediatly the scheduler will start interpreting
    * the workflow and generate [[Execution Executions]] sent to the provided [[Executor]].
    *
    * @param jobs The jobs to run in this case in a DAG representation
    * @param executor The executor to use to run the generated [[Execution Executions]].
    * @param xa The doobie transactor to use to persist the scheduler state if needed.
    * @param logger The logger to use to log internal debug state if needed.
    */

  def startStream(jobs: Workload[FlowScheduling], executor: Executor[FlowScheduling], xa: XA, logger: Logger): fs2.Stream[IO, Either[Throwable, Set[RunJob]]] = {
    val workflow = initialize(jobs, xa, logger)

    fs2
      .Stream
      .eval(runJobs(workflow, executor, xa, Set.empty)) // Init
      .through(trampoline(firstFinished(workflow, executor, xa), o => o.isLeft || o.toOption.get.isEmpty))
  }


  private def firstFinished(workflow : FlowWorkflow, executor: Executor[FlowScheduling], xa: XA)
                            (possiblyRunning : Either[Throwable, Set[RunJob]]) : IO[Either[Throwable, Set[RunJob]]] =
    possiblyRunning match {
      case Right(running) =>

        IO.async[IO[Either[Throwable, Set[RunJob]]]] { callback =>
          Future.firstCompletedOf(running.map { case (_, _, done) => done }).onComplete { _ => // We always callback with Right because Left is managed by error job in runJobs
            callback(Right(runJobs(workflow, executor, xa, running)))
          }
        }.flatten

      case Left(error) => IO(Left(error))
  }


}