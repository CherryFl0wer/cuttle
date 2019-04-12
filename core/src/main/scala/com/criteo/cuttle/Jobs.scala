package com.criteo.cuttle

import scala.concurrent.Future
import io.circe._
import io.circe.syntax._
import cats.Eq
import io.circe.Encoder._
import io.circe.generic.auto._

/** Allow to tag a job. Tags can be used in the UI/API to filter jobs
  * and more easily retrieve them.
  *
  * @param name Tag name as displayed in the UI.
  * @param description Description as displayed in the UI.
  */
case class Tag(name: String, description: String = "")



/** The job [[SideEffect]] is the most important part as it represents the real
  * job logic to execute. A job is defined for a given [[Scheduling]],
  * for example it can be a [[timeseries.TimeSeries TimeSeries]] job. Jobs are also [[Workflow]] with a
  * single vertice.
  *
  * @tparam S The kind of [[Scheduling]] used by this job.
  * @param id The internal job id. It will be sued to track the job state in the database, so it must not
  *           change over time otherwise the job will be seen as a new one by the scheduler.
  *           That id, being technical, should only use valid characters such as [a-zA-Z0-9_-.]
  * @param scheduling The scheduling configuration for the job. For example a [[timeseries.TimeSeries TimeSeries]] job can
  *                   be configured to be hourly or daily, etc.
  * @param name The job name as displayed in the UI.
  * @param description The job description as displayed in the UI.
  * @param tags The job tags used to filter jobs in the UI.
  * @param effect The job side effect, representing the real job execution.
  */


sealed trait JobKind
case object NormalJob extends JobKind
final case class SignalJob(eventTrigger : String) extends JobKind

final case class Job[S <: Scheduling](id: String,
                                scheduling: S,
                                description: String = "",
                                kind : JobKind = NormalJob,
                                tags: Set[Tag] = Set.empty[Tag])
                               (val effect: SideEffect[S]) {
  /** Run this job for the given [[Execution]].
    *
    * @param execution The execution instance.
    * @return A future indicating the execution result (Failed future means failed execution).
    */
  private[cuttle] def run(execution: Execution[S]): Future[Completed] =
    effect(execution)
}

/** Companion object for [[Job]]. */
case object Job {
  implicit def eqInstance[S <: Scheduling] =
    Eq.fromUniversalEquals[Job[S]]
}

/** Represent the workload of a Cuttle project, ie. the list of
  * jobs to be scheduled in some ways. */
trait Workload[S <: Scheduling] {
  /** All known jobs in this workload. */
  def all: Set[Job[S]]
  /** Represent the jobs as JSON. */
  def asJson(implicit se : Encoder[Job[S]]): Json = {
    val jobs = all.asJson
    val tags = all.flatMap(_.tags).asJson
    val dependencies = Json.arr()
    Json.obj(
      "jobs" -> jobs,
      "dependencies" -> dependencies,
      "tags" -> tags
    )
  }
}