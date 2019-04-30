package com.criteo.cuttle.flow

import java.time.{Instant}
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle._
import doobie.implicits._
import io.circe._
import io.circe.syntax._

import scala.concurrent.{Future}
import scala.concurrent.stm.Txn.ExternalDecider
import scala.concurrent.stm._



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


  //TODO Adapt it no more exit and no print on the stack.
  private def runOrLogAndDie(thunk: => Unit, message: => String): Unit = {
    import java.io._

    try {
      thunk
    } catch {
      case (e: Throwable) => {
        logger.error(message)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        System.exit(-1)
      }
    }
  }

  private def currentJobsRunning(state : State) : Set[FlowJob] = atomic { implicit txn =>
    state.filter { case (_, jobState) =>
      jobState match {
        case Done(_) => false
        case _ => true
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
        case Failed(_) => false
        case _ => true
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

  /*
  * Wrap around IO
  * */
  private[flow] def initialize(wf : Workload[FlowScheduling], xa : XA, logger : Logger) = {
    val workflow = wf.asInstanceOf[FlowWorkflow]

    logger.info("Validate flow workflow before start")

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
                              state : State): Seq[Executable] = {


    val doneJob = currentJobsDone(state)
    val newWorkflow = FlowWorkflow.without(workflow, doneJob)

    // Are those which are not running
    def jobsAllowedToRun(nextJobs : Set[FlowJob]) =
      nextJobs
        .filter { job => !currentJobsRunning(state).contains(job) }
        .foldLeft(Set.empty[FlowJob]) { (acc, job) =>
          if (currentJobsFailed(state).contains(job)) { // if the jobs has failed then we give its error path for next jobs
            val errorChild = newWorkflow.childsFromRoute(job, RoutingKind.Failure)
            acc ++ errorChild
          } else acc + job // Normal success job
        }


    val roots = newWorkflow.roots //.filter(job => newWorkflow.isA(RoutingKind.Success)(job))
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
                            xa : XA, running : Set[RunJob]) : Set[RunJob] = {


    val (completed, stillRunning) = running.partition {
      case (_, _, effect) => effect.isCompleted
    }

    // Update state and get the jobs to run
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

    val newExecutions = executor.runAll(toRun)

    atomic { implicit txn =>
      _state() = newExecutions.foldLeft(_state()) {
        case (st, (execution, _)) =>
          st + (execution.job -> Running(execution.id))
      }
    }

    if (completed.nonEmpty || toRun.nonEmpty) {
      runOrLogAndDie(Database.serializeState(workflowdId, stateSnapshot, None).transact(xa).unsafeRunSync,
        "FlowScheduler, cannot serialize state, shutting down")
    }

    val statusJobs = stillRunning ++ newExecutions.map {
      case (execution, result) =>
        (execution.job, execution.context, result)
    }

    statusJobs
  }


  /***
    *
    * @param jobs The jobs to run in this case in a DAG representation
    * @param executor The executor to use to run the generated [[Execution Executions]].
    * @param xa The doobie transactor to use to persist the scheduler state if needed.
    * @param logger The logger to use to log internal debug state if needed.
    */

  def start(jobs: Workload[FlowScheduling], executor: Executor[FlowScheduling], xa: XA, logger: Logger) : Unit = ()

  def start[F[_] : Sync](jobs: Workload[FlowScheduling],
            executor: Executor[FlowScheduling],
            xa: XA,
            logger: Logger)(implicit C : Concurrent[F]): fs2.Stream[F, Set[RunJob]] = {

    val wf = initialize(jobs, xa, logger)
    val currentExecution = fs2.Stream(runJobs(wf, executor, xa, Set.empty))

    currentExecution
      .covary[F]
      .through(trampoline(doneFirst[F](wf, executor, xa), (rj : Set[RunJob]) => rj.isEmpty))
  }


  private def doneFirst[F[_] : Sync](wf : FlowWorkflow, executor: Executor[FlowScheduling], xa: XA)
                            (running : Set[RunJob])
                            (implicit F: Concurrent[F]): F[Set[RunJob]] = F.async {
    cb =>
      Future
        .firstCompletedOf(running.map { case (_, _, done) => done })
        .onComplete {
          case _ => cb(Right(runJobs(wf, executor, xa, running)))
        }
  }

  //@Todo F : Sync
  /*private[flow] def pauseJobs(jobs: Set[Job[FlowScheduling]], executor: Executor[FlowScheduling], xa: XA): Unit = {
    val executionsToCancel = atomic { implicit tx =>
      val pauseDate = Instant.now()
      val pausedJobIds = _pausedJobs().map(_.id)
      val jobsToPause: Set[PausedJob] = jobs
        .filter(job => !pausedJobIds.contains(job.id))
        .map(job => PausedJob(job.id, pauseDate))

      if (jobsToPause.isEmpty) return

      _pausedJobs() = _pausedJobs() ++ jobsToPause

      val pauseQuery = jobsToPause.map(queries.pauseJob).reduceLeft(_ *> _)
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          pauseQuery.transact(xa).unsafeRunSync
          true
        }
      })

      jobsToPause.flatMap { pausedJob =>
        executor.runningState.filterKeys(_.job.id == pausedJob.id).keys ++ executor.throttledState
          .filterKeys(_.job.id == pausedJob.id)
          .keys
      }
    }
    logger.debug(s"we will cancel ${executionsToCancel.size} executions")
    executionsToCancel.toList.sortBy(_.context).reverse.foreach { execution =>
      execution.streams.debug(s"Job has been paused")
      execution.cancel()
    }
  }*/

}