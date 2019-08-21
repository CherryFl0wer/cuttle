package com.criteo.cuttle.flow

import java.time.Instant

import cats.data.EitherT
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
import com.criteo.cuttle.flow.utils.JobUtils

import scala.collection.mutable
import scala.collection.mutable.{LinkedHashSet, ListBuffer}




/** A [[FlowScheduler]] executes the [[com.criteo.cuttle.flow.FlowWorkflow Workflow]]
  */
case class FlowScheduler(logger: Logger,
                         workflowId : String,
                         refState : CatsRef[IO, JobState],
                         refWorkflow : CatsRef[IO, FlowWorkflow],
                         hashWorkflow : Int) extends Scheduler[FlowScheduling] {


  override val name = "flow"

  private val discardedJob = new mutable.LinkedHashSet[FlowJob]()

  private def currentJobsRunning(state : JobState) : Set[FlowJob] =
    state.filter { case (_, jobState) =>
      jobState match {
        case Running(_) => true
        case _          => false
      }
    }.keySet


  private def currentJobsDone(state : JobState) : Set[FlowJob] =
    state.filter { case (_, jobState) =>
      jobState match {
        case Done => true
        case _    => false
      }
    }.keySet


  private def currentJobsFailed(state : JobState) : Set[FlowJob] =
    state.filter { case (_, jobState) =>
      jobState match {
        case Failed => true
        case _      => false
      }
    }.keySet

  /**
      Save job's result in the database.
    * @param job the job to save
    * @param xa doobie sql
    * */
  private def saveResult(job : FlowJob, xa : XA) = for {
      _ <- Database
        .insertResult(hashWorkflow.toString, workflowId, job.id,  job.scheduling.inputs.asJson, job.scheduling.outputs)
        .transact(xa)
  } yield ()


  /**
    * @param running jobs set
    * @return tuple of completed and still running job
    */
  private def completion(running : Set[RunJob]) =  running.partition {
    case (_, _, effect) => effect.isCompleted
  }



  private[flow] def initialize(workflow : FlowWorkflow, xa : XA, logger : Logger)= for {
      _ <- EitherT.right[Throwable](logger.info("Validate flow workflow before start"))
      _ <- EitherT(IO.pure(FlowSchedulerUtils.validate(workflow).leftMap(x => new Throwable(x.mkString("\n")))))
      _ <- EitherT.right[Throwable](logger.info("Flow Workflow is valid"))
      _ <- EitherT.right[Throwable](logger.info("Update state"))
      maybeState <- EitherT(Database.deserializeState(workflowId)(workflow.vertices).transact(xa).attempt)
      _ <- EitherT((maybeState match {
        case Some(jobsState) => refState.set(jobsState.filterNot(_._2 == Failed))
        case _ => IO.unit
      }).attempt)
    } yield ()


  /**
     Select the jobs that will run
    * @param workflow To get the next jobs to run and the result from previous node
    * @param executor To get data for context job
    * @param newState Current state of the jobs
    * @return A sequence of executable jobs with their new formatted new inputs
    */

  private[flow] def jobsToRun(workflow: FlowWorkflow,
                              executor: Executor[FlowScheduling],
                              newState : JobState): Seq[Executable] = {

    // Jobs to run are those which are not running
    def jobsAllowedToRun(nextJobs : Set[FlowJob]) = nextJobs
        .diff(currentJobsRunning(newState))
        .foldLeft(Set.empty[FlowJob]) { (acc, job) =>
          if (currentJobsFailed(newState).contains(job)) { // if the jobs has failed then we give its error path for next jobs
            workflow.pathFromVertice(job, RoutingKind.Success).foreach(discardedJob.add)
            discardedJob.add(job)

            val errorChild = workflow.childsFromRoute(job, RoutingKind.Failure)
            if (errorChild.isEmpty) acc else acc ++ errorChild
          }
          else
            acc + job // Normal success job
        }

    // Next jobs ? take off jobs done, discarded ones and error job
    // Error job will be added by jobsAllowedToRun
    val jobsToRemove = currentJobsDone(newState) ++ discardedJob.toSet ++ workflow.childFrom(RoutingKind.Failure)
    val newWorkflow = FlowWorkflow.without(workflow, jobsToRemove)
    val jobs = jobsAllowedToRun(newWorkflow.roots)

    jobs.map { currentJob =>
      val json = FlowSchedulerUtils.mergeDuplicateJson(
        (currentJob.id, currentJob.scheduling.inputs),
        workflow.parentsOf(currentJob).map(job => (job.id, job.scheduling.outputs)).toList
      )._2
      val newInputs = FlowSchedulerUtils.formatKeyOnJson(json, JobUtils.formatName)

      val jobWithInput = currentJob.copy(scheduling = FlowScheduling(inputs = newInputs))(currentJob.effect)
      (jobWithInput, FlowSchedulerContext(Instant.now, workflowId))
    }.toSeq
  }



  /**
    * Run the jobs and update state of the scheduler
    * @param executor Execute the side effect of a job
    * @param xa doobie sql
    * @param running Set of current job running (can have completed jobs)
    * */
  private[flow] def runJobs(executor: Executor[FlowScheduling],
                            xa : XA,
                            running : Set[RunJob]): IO[Either[Throwable, Set[RunJob]]] = {

    val (completed, stillRunning) = completion(running)

    for {
      workflow <- refWorkflow.get
      stateMap <- refState.get
      jobToUpdateOutput = ListBuffer.empty[FlowJob]
      updatedJob <- completed.flatMap {
        case (job, _, future) => future.value.get match { // Check jobs status and save into the db
          case status if status.isSuccess || (stateMap.get(job).isDefined && stateMap(job) == Done) =>
            status.get match { // Set output of the current job
              case Output(res) =>
                val jobWithOutput = job.copy(scheduling = FlowScheduling(inputs = job.scheduling.inputs, outputs = res))(job.effect)
                jobToUpdateOutput.append(jobWithOutput)
                Some(saveResult(jobWithOutput, xa).map(_ => jobWithOutput -> Done))
              case OutputErr(err) =>
                val jobWithError = job.copy(scheduling = FlowScheduling(inputs = job.scheduling.inputs, outputs = err))(job.effect)
                jobToUpdateOutput.append(jobWithError)
                Some(saveResult(jobWithError, xa).map(_ => jobWithError -> Failed))

              case Finished => Some(saveResult(job, xa).map(_ => job -> Done))
              case Fail => Some(saveResult(job, xa).map(_ => job -> Failed))
            }
          case _ => Some(saveResult(job, xa).map(_ => job -> Failed))
        }
      }.toList.traverse(identity)

      _ <- refWorkflow.set(jobToUpdateOutput.toList.foldLeft(workflow) { (acc, job) => FlowWorkflow.replace(acc, job) })
      updatedMapJob = updatedJob.toMap

      _ <- refState.update { st =>  st ++ updatedMapJob }
      stateSnapshot   <- refState.get
      workflowUpdated <- refWorkflow.get
      runningSeq    = jobsToRun(workflowUpdated, executor, stateSnapshot)
      newExecutions <- IO.delay(executor.runAll(runningSeq))

      // Add execution to state
      execState = newExecutions.map { case (exec, _) => exec.job -> Running(exec.id) }.toMap
      _         <- refState.update(st => st ++ execState)

      statusJobs = stillRunning ++ newExecutions.map { case (exec, res) => (exec.job, exec.context, res) }

      jobStatus <- if (completed.nonEmpty || runningSeq.nonEmpty)
        Database.serializeState(workflowId, stateSnapshot, None)
        .map(_ => statusJobs)
        .value
        .transact(xa)
      else
        EitherT.rightT[IO, Throwable](statusJobs).value
    } yield jobStatus

  }


  /**
  Starts the scheduler for the given ref Workflow. Immediately the scheduler will start interpreting
  the workflow and generate [[Execution Executions]] sent to the provided [[Executor]].
    * @param executor The executor to use to run the generated [[Execution Executions]].
    * @param xa The doobie transactor to use to persist the scheduler state if needed.
    * @param logger The logger to use to log internal debug state if needed.
    */


  def executeWorkflow(executor: Executor[FlowScheduling],
                      xa: XA,
                      logger: Logger): EitherT[IO, Throwable, (FlowWorkflow, JobState)] = {

    for {
      workflow <- EitherT.right[Throwable](refWorkflow.get)
      _     <- initialize(workflow, xa, logger) //TODO check
      _     <- runWorkflow(executor, xa)(Set.empty)
      state <- EitherT.right[Throwable](refState.get)
      wf    <- EitherT.right[Throwable](refWorkflow.get)
    } yield (wf, state)
  }




  /**
    * Run workflow future
    * @param executor
    * @param xa
    * @param jobsStatus
    * @return
    */
  private def runWorkflow(executor: Executor[FlowScheduling], xa: XA)
                         (jobsStatus : Set[RunJob]) : EitherT[IO, Throwable, Unit]
  = for {
      jobs <- EitherT(runJobs(executor, xa, jobsStatus))
      state <- EitherT.right[Throwable](refState.get)
      _ <-
        if (jobs.isEmpty && currentJobsFailed(state).nonEmpty)
          EitherT.leftT[IO, Unit](new Throwable(s"Runtime Error, Job ${currentJobsFailed(state).map(_.id).mkString(" and ")} failed"))
        else if(jobs.isEmpty)
          EitherT.rightT[IO, Throwable](())
        else
          EitherT.right[Throwable](IO.fromFuture(IO(Future.firstCompletedOf(jobs.map { case (_, _, done) => done })))
              .handleErrorWith { _ => IO.unit }) *> runWorkflow(executor, xa)(jobs)
    } yield ()


}