package com.criteo.cuttle.flow

import java.time.{Duration, Instant, ZoneId}
import java.util.{Comparator, UUID}

import cats._
import cats.implicits._
import com.criteo.cuttle._
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle.flow.FlowSchedulerUtils._
import doobie.free.connection.ConnectionIO
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.stm._
import doobie.implicits._



/**
  * @date Thursday 19 March 2019
*  Works like TimeSeries in general but does not depends on a calendar.
  *  It is in the same package as TimeSeries because it uses Graph as a base
  *   .When a job has finished its execution save its output in the DB
  *   .Then wait for the other jobs in Parallel to finish to send the result to the 'dependent'
  *   job if there is one.
* */



case class FlowSchedulerContext(start : Instant,
                                projectVersion: String = "",
                                workflowId : String,
                                inputs: Option[Json]) extends SchedulingContext {

  override def asJson: Json = FlowSchedulerContext.encoder(this)

  def toId: String = {
    s"${start}${workflowId}${UUID.randomUUID().toString}"
  }

  override def logIntoDatabase: ConnectionIO[String] = Database.serializeContext(this)

  def compareTo(other: SchedulingContext) = other match {
    case FlowSchedulerContext(timestamp, _, _, _) => start.compareTo(timestamp) // Priority compare // TODO
  }

}


// Decoder / Encoder
object FlowSchedulerContext {

  // TODO: because apparently it does not detect encoding instant
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
        "inputs" -> a.inputs.asJson

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
  */
case class FlowScheduling() extends Scheduling {

  type Context = FlowSchedulerContext

  override def asJson: Json = Json.obj(
    "kind" -> "flow".asJson
  )
}

private[flow] sealed trait JobFlowState

private[flow] case class Done(projectVersion: String) extends JobFlowState
private[flow] case class Running(executionId: String) extends JobFlowState
private[flow] case class Todo(executionId: String) extends JobFlowState


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
case class FlowScheduler(logger: Logger) extends Scheduler[FlowScheduling] {
  import FlowSchedulerUtils._

  override val name = "flow"

  // State of a job
  private val _state = Ref(Map.empty[FlowJob, JobFlowState])

  private[flow] def state: State = atomic { implicit txn =>
    _state()
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
      .deserializeState(workflow.vertices)
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

    val parentMap = workflow.edges.groupBy { case (child, _, _) => child }
    val jobsList = workflow.jobsInOrder.toSet


    val toRun = jobsList.diff(parentMap.keySet).map { j =>
      (j, FlowSchedulerContext(Instant.now, workflow.uid.toString, executor.projectVersion, None))
    }

    toRun.toSeq
  }

  private[flow] def runJobAndGetNextOnes(running : Set[RunJob],
                                         workflow: FlowWorkflow,
                                         executor: Executor[FlowScheduling],
                                         xa : XA) : Set[RunJob] = {


    val (completed, stillRunning) = running.partition {
      case (_, _, effect) => effect.isCompleted
    }

    val (stateSnapshot, toRun) = atomic { implicit txn =>

      def isDone(state: State, job: FlowJob, context: FlowSchedulerContext): Boolean =
        state.apply(job) match  {
          case Done(_) => true
          case _       => false
        }

      // update state with job statuses
      val newState = completed.foldLeft(state) {
        case (acc, (job, context, future)) =>
          val jobState =
            if (future.value.get.isSuccess || isDone(_state(), job, context)) Done(context.projectVersion)
            else Todo(s"${job.id}")
          acc + (job -> jobState)
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
      runOrLogAndDie(Database.serializeState(stateSnapshot, None).transact(xa).unsafeRunSync,
        "FlowScheduler, cannot serialize state, shutting down")
    }

    Set.empty
  }

  override def start(jobs: Workload[FlowScheduling],
                     executor: Executor[FlowScheduling],
                     xa: XA,
                     logger: Logger): Unit = {

    val wf = initialize(jobs, xa, logger)

    def mainLoop(running: Set[RunJob]): Unit = {
      val newRunning = runJobAndGetNextOnes(running, wf, executor, xa)
      utils.Timeout(ScalaDuration.create(1, "s")).andThen { case _ => mainLoop(newRunning) }
    }

    mainLoop(Set.empty)
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
