package com.criteo.cuttle.flow

import java.time.Instant

import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle._
import com.criteo.cuttle.flow.FlowSchedulerUtils._

import doobie.implicits._
import io.circe._
import io.circe.syntax._

import scala.concurrent.Future

import cats.effect.concurrent.{Ref => CatsRef}
import cats.effect.IO
import cats.implicits._

import scala.collection.mutable.LinkedHashSet


/** A [[FlowScheduler]] executes the [[com.criteo.cuttle.flow.FlowWorkflow Workflow]]
  */
case class FlowScheduler(logger: Logger,
                         workflowdId : String,
                         refState : CatsRef[IO, JobState],
                         refResults : CatsRef[IO, JobResults]) extends Scheduler[FlowScheduling] {

  override val name = "flow"

  private val queries = Queries(logger)

  private val discardedJob = new LinkedHashSet[FlowJob]()

  private def currentJobsRunning(state : JobState) : Set[FlowJob] =
    state.filter { case (_, jobState) =>
      jobState match {
        case Running(_) => true
        case _ => false
      }
    }.keySet


  private def currentJobsDone(state : JobState) : Set[FlowJob] =
    state.filter { case (_, jobState) =>
      jobState match {
        case Done => true
        case _ => false
      }
    }.keySet


  private def currentJobsFailed(state : JobState) : Set[FlowJob] =
    state.filter { case (_, jobState) =>
      jobState match {
        case Failed => true
        case _ => false
      }
    }.keySet




  /**
    *  Save job's result in the database and in a map inmemory.
    *  the map is here to avoid seeking for the result in the db every time we need it
    * @param wfHash workflow hash
    * @param job the job to save
    * @param context the context of the job
    * @param xa doobie sql
    * */
  private def saveResult(wfHash : Int, job : FlowJob, context : FlowSchedulerContext, xa : XA) = for {
      _ <- Database
        .insertResult(wfHash.toString, workflowdId, job.id,  job.scheduling.inputs.asJson, context.result)
        .transact(xa) // Attempt in case of err
      _ <- refResults.modify(m => (m + (job -> context.result), m))
  } yield ()


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
    import cats.data.EitherT

    for {
      _ <- EitherT(IO.pure(FlowSchedulerUtils.validate(workflow).leftMap(x => new Throwable(x.mkString("\n")))))
      _ = logger.info("Flow Workflow is valid")
      _ = logger.info("Update state")
      maybeState <- EitherT(Database.deserializeState(workflowdId)(workflow.vertices).transact(xa).attempt)
      _ = if (maybeState.isDefined) refState.set(maybeState.get)
    } yield workflow
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
                              newState : JobState): IO[Seq[Executable]] = {

    // Jobs to run are those which are not running
    def jobsAllowedToRun(nextJobs : Set[FlowJob]) = nextJobs
        .diff(currentJobsRunning(newState))
        .foldLeft(Set.empty[FlowJob]) { (acc, job) =>
          if (currentJobsFailed(newState).contains(job)) { // if the jobs has failed then we give its error path for next jobs
            workflow.pathFromVertice(job, RoutingKind.Success).foreach(discardedJob.add)
            discardedJob.add(job)

            val errorChild = workflow.childsFromRoute(job, RoutingKind.Failure)
            errorChild.isEmpty match {
              case true => acc
              case _ => acc ++ errorChild
            }
          }
          else
            acc + job // Normal success job
        }


    // Next jobs ? take off jobs done, discarded ones and error job
    // Error job will be added by jobsAllowedToRun
    val newWorkflow = FlowWorkflow.without(workflow, currentJobsDone(newState) ++ discardedJob.toSet ++ workflow.childFrom(RoutingKind.Failure))
    val toRun = jobsAllowedToRun(newWorkflow.roots).map { j => for {
        resultsMap <- refResults.get
        parentOfJob = workflow.parentsOf(j) // Previous job
        resultsFromParent = parentOfJob.map { job => job.id -> resultsMap.getOrElse(job, Json.Null) }.toMap
        optResults = if (resultsFromParent.isEmpty) None else Some(resultsFromParent)
      } yield (j, FlowSchedulerContext(Instant.now, executor.projectVersion, workflowdId, optResults))
    }

    toRun.toList.traverse(identity)
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
    for {
      stateMap <- refState.get
      updatedJob <- completed.flatMap { case (job, ctx, future) =>
        val jobExist = stateMap.get(job).isDefined
        future.value.get match {
          case x if x.isSuccess || jobExist && stateMap(job) == Done => Some(saveResult(workflow.hash, job, ctx, xa).map(_ => job -> Done))
          case x if x.isFailure => Some(saveResult(workflow.hash, job, ctx, xa).map(_ => job -> Failed))
          case _ => None
        }
      }.toList.traverse(identity)

      updatedMapJob = updatedJob.toMap

      _ <- refState.update { st =>  st ++ updatedMapJob }
      stateSnapshot <- refState.get
      runningSeq <- jobsToRun(workflow, executor, stateSnapshot)

      newExecutions = executor.runAll(runningSeq) // TODO Change core to return IO execution
      // Add execution to state
      execState = newExecutions.map { case (exec, _) => exec.job -> Running(exec.id) }.toMap
      _ <- refState.update(st => st ++ execState)

      statusJobs = stillRunning ++ newExecutions.map { case (exec, res) => (exec.job, exec.context, res) }
      newStatus <- if(completed.nonEmpty || runningSeq.nonEmpty)
          Database
           .serializeState(workflowdId, stateSnapshot, None).transact(xa).map(_ => statusJobs).attempt
        else
          IO.pure(Either.right(statusJobs))
    } yield newStatus

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

  def startStream(jobs: Workload[FlowScheduling],
                  executor: Executor[FlowScheduling],
                  xa: XA,
                  logger: Logger) = {

    for {
       workflowStream <- fs2.Stream.eval(initialize(jobs, xa, logger).value)
       workflow <- cats.data.EitherT(IO.pure(workflowStream))

       runningJobs = fs2.Stream.eval(runJobs(workflow, executor, xa, Set.empty)) // Init
       results <- runningJobs.through(trampoline(firstFinished(workflow, executor, xa), jobs => jobs.isLeft || jobs.toOption.get.isEmpty))
    } yield results

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