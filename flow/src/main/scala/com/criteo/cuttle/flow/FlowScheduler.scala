package com.criteo.cuttle.flow

import java.time.{Instant, ZoneId}
import java.util.{Comparator, UUID}

import cats._
import cats.implicits._
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle._
import com.criteo.cuttle.flow.FlowSchedulerUtils._
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

import scala.concurrent.Future
import scala.concurrent.stm.Txn.ExternalDecider
import scala.concurrent.stm._

case class FlowSchedulerContext(start : Instant,
                                projectVersion: String = "",
                                workflowId : String,
                                resultsFromPreviousNodes : Map[String, Json] = Map.empty) extends SchedulingContext {

  var result : Json = Json.Null // Response of the job that will be saved in the database and store in a map during exec

  override def asJson: Json = FlowSchedulerContext.encoder(this)

  override def logIntoDatabase: ConnectionIO[String] = Database.serializeContext(this)

  def toId: String = s"${start}-${workflowId}-${UUID.randomUUID().toString}"

  def compareTo(other: SchedulingContext) = other match {
    case FlowSchedulerContext(timestamp, _, _, _) => start.compareTo(timestamp) // Priority compare
  }

}


// Decoder / Encoder
object FlowSchedulerContext {

  implicit val encodeInstant: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => "Instant")
  }

  private[flow] implicit val encoder: Encoder[FlowSchedulerContext] = new Encoder[FlowSchedulerContext] {
    override def apply(a: FlowSchedulerContext): Json = {
      Json.obj(
        "startAt" -> a.start.asJson,
        "workflowId" -> a.workflowId.asJson,
        "projectVersion" -> a.projectVersion.asJson,
        "cascadingResults" -> a.resultsFromPreviousNodes.asJson
      )
    }
  }
  private[flow] implicit def decoder(implicit jobs: Set[FlowJob]): Decoder[FlowSchedulerContext] = deriveDecoder

  /** Provide an implicit `Ordering` for [[FlowSchedulerContext]] based on the `compareTo` function. */
  implicit val ordering: Ordering[FlowSchedulerContext] =
    Ordering.comparatorToOrdering(new Comparator[FlowSchedulerContext] {
      def compare(o1: FlowSchedulerContext, o2: FlowSchedulerContext) = o1.compareTo(o2)
    })
}



/**
  * A [[FlowScheduling]] has a role of defining the configuration of the scheduler
  * @param inputs Supposed to be the params that can be used inside a job
  *               Currently defined by a string containing a parsable json object
  */
case class FlowScheduling(inputs : Option[String] = None) extends Scheduling {

  type Context = FlowSchedulerContext

  override def asJson: Json = Json.obj(
    "kind" -> "flow".asJson,
    "inputs" -> inputs.asJson
  )
}

private[flow] sealed trait JobFlowState
private[flow] case class Done(projectVersion: String) extends JobFlowState
private[flow] case class Running(executionId: String) extends JobFlowState

private[flow] object JobFlowState {
  implicit val doneEncoder: Encoder[Done] = new Encoder[Done] {
    def apply(done: Done) =
      Json.obj(
        "v" -> done.projectVersion.asJson
      )
  }

  implicit val doneDecoder: Decoder[Done] = new Decoder[Done] {
    def apply(c: HCursor): Decoder.Result[Done] =
      for {
        version <- c.downField("v").as[String]
          .orElse(c.downField("projectVersion").as[String])
          .orElse(Right("no-version"))
      } yield Done(version)
  }


  implicit val encoder: Encoder[JobFlowState] = deriveEncoder
  implicit def decoder(implicit jobs: Set[JobFlowState]): Decoder[JobFlowState] = deriveDecoder
  implicit val eqInstance: Eq[JobFlowState] = Eq.fromUniversalEquals[JobFlowState]
}




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
    state.filter { p =>
      p._2 match {
        case Done(_) => false
        case _ => true
      }
    }.keySet
  }

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

    workflow
  }


  private[flow] def jobsToRun(workflow: FlowWorkflow,
                              executor: Executor[FlowScheduling],
                              state : State): Seq[Executable] = {

    val newWorkflow = FlowWorkflow without(workflow, state.keySet)

    def jobsAllowedToRun(nextJobs : Set[FlowJob], runningJobs : Set[FlowJob]) =
      nextJobs
        .filter { job =>
          workflow.edges
          .filter { case (parent, _) => parent == job }
          .foldLeft(true)( (acc, edge) => acc && !runningJobs.contains(edge._2))
      }


    val toRun = jobsAllowedToRun(newWorkflow.roots, currentJobsRunning(state)).map { j =>
      val childsOfjob = workflow.childOf(j)
      val resultsOfChild = childsOfjob.foldLeft(Map.empty[String, Json])((acc, job) => atomic {
        implicit txn => acc + (job.id -> _results().getOrElse(job, Json.Null))
      })

      (j, FlowSchedulerContext(Instant.now, executor.projectVersion, workflowdId, resultsOfChild))
    }

    toRun.toSeq
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


  private[flow] def runJobAndGetNextOnes(running : Set[RunJob],
                                         workflow: FlowWorkflow,
                                         executor: Executor[FlowScheduling],
                                         xa : XA) : Set[RunJob] = {


    val (completed, stillRunning) = running.partition {
      case (_, _, effect) => effect.isCompleted
    }

    // Update state and get the jobs to run
    val (stateSnapshot, toRun) = atomic { implicit txn =>

      def isDone(state: State, job: FlowJob): Boolean = state.apply(job) match  {
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

  override def start(jobs: Workload[FlowScheduling],
                     executor: Executor[FlowScheduling],
                     xa: XA,
                     logger: Logger): Unit = {

    import scala.async.Async.{async, await}

    val wf = initialize(jobs, xa, logger)
    def mainLoop(running: Set[RunJob]): Unit = {

      val newRunning = runJobAndGetNextOnes(running, wf, executor, xa)

      if (!newRunning.isEmpty) async
      {
        val ft = Future.firstCompletedOf(newRunning.map { case (_, _, f) => f })
        await(ft)
      }.onComplete { f => // TODO: Fail future management
        mainLoop(newRunning)
      }

    }

    mainLoop(Set.empty)
  }


  private[flow] def pauseJobs(jobs: Set[Job[FlowScheduling]], executor: Executor[FlowScheduling], xa: XA): Unit = {
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
  }

}






object FlowSchedulerUtils {
  type FlowJob = Job[FlowScheduling]

  type Executable = (FlowJob, FlowSchedulerContext) // A job to be executed
  type RunJob = (FlowJob, FlowSchedulerContext, Future[Completed]) // Job, Context, Result
  type State = Map[FlowJob, JobFlowState]

  val UTC: ZoneId = ZoneId.of("UTC")

  /**
    * Validation of:
    * - absence of cycles in the workflow, implemented based on Kahn's algorithm
    * - absence of jobs with the same id
    * @param workflow workflow to be validated
    * @return either a validation errors list or a unit
    */
  def validate(workflow: FlowWorkflow): Either[List[String], Unit] = {
    val errors = collection.mutable.ListBuffer(FlowWorkflow.validate(workflow): _*)
    if (errors.nonEmpty) Left(errors.toList)
    else Right(())
  }

}
