package com.criteo.cuttle.timeseries

import java.time.Instant
import java.time.temporal.ChronoUnit
import cats.implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.syntax._

import com.criteo.cuttle.ExecutionStatus._
import com.criteo.cuttle._
import com.criteo.cuttle.timeseries.TimeSeriesUtils._
import com.criteo.cuttle.timeseries.intervals.Bound.{Bottom, Finite, Top}
import com.criteo.cuttle.timeseries.intervals._


private[timeseries] object TimeSeriesApp {

  implicit def projectEncoder = new Encoder[CuttleProject] {
    override def apply(project: CuttleProject) =
      Json.obj(
        "name" -> project.name.asJson,
        "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
        "description" -> Option(project.description).filterNot(_.isEmpty).asJson,
        "scheduler" -> "timeseries".asJson,
        "env" -> Json.obj(
          "name" -> Option(project.env._1).filterNot(_.isEmpty).asJson,
          "critical" -> project.env._2.asJson
        )
      )
  }

  implicit val executionStatEncoder: Encoder[ExecutionStat] = new Encoder[ExecutionStat] {
    override def apply(execution: ExecutionStat): Json =
      Json.obj(
        "startTime" -> execution.startTime.asJson,
        "endTime" -> execution.endTime.asJson,
        "durationSeconds" -> execution.durationSeconds.asJson,
        "waitingSeconds" -> execution.waitingSeconds.asJson,
        "status" -> (execution.status match {
          case ExecutionSuccessful => "successful"
          case ExecutionFailed     => "failed"
          case ExecutionRunning    => "running"
          case ExecutionWaiting    => "waiting"
          case ExecutionPaused     => "paused"
          case ExecutionThrottled  => "throttled"
          case ExecutionTodo       => "todo"
        }).asJson
      )
  }

  implicit val tagEncoder = new Encoder[Tag] {
    override def apply(tag: Tag) =
      Json.obj(
        "name" -> tag.name.asJson,
        "description" -> Option(tag.description).filterNot(_.isEmpty).asJson
      )
  }

  implicit def jobEncoder = new Encoder[Job[TimeSeries]] {
    override def apply(job: Job[TimeSeries]) =
      Json
        .obj(
          "id" -> job.id.asJson,
          "name" -> Option(job.name).filterNot(_.isEmpty).getOrElse(job.id).asJson,
          "description" -> Option(job.description).filterNot(_.isEmpty).asJson,
          "scheduling" -> job.scheduling.asJson,
          "tags" -> job.tags.map(_.name).asJson
        )
        .asJson
  }

}

private[timeseries] case class TimeSeriesApp(project: CuttleProject,
                                             executor: Executor[TimeSeries],
                                             scheduler: TimeSeriesScheduler,
                                             xa: XA) {

  import project.{jobs}

  import JobState._
  import TimeSeriesApp._
  import TimeSeriesCalendar._

  private val allIds = jobs.all.map(_.id)

  private def parseJobIds(jobsQueryString: String): Set[String] =
    jobsQueryString.split(",").filter(_.trim().nonEmpty).toSet


  private implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    implicit val boundEncoder = new Encoder[Bound[Instant]] {
      override def apply(bound: Bound[Instant]) = bound match {
        case Bottom    => "-oo".asJson
        case Top       => "+oo".asJson
        case Finite(t) => t.asJson
      }
    }

    override def apply(interval: Interval[Instant]) =
      Json.obj(
        "start" -> interval.lo.asJson,
        "end" -> interval.hi.asJson
      )
  }
  private val queries = Queries(project.logger)

  private trait ExecutionPeriod {
    val period: Interval[Instant]
    val backfill: Boolean
    val aggregated: Boolean
    val version: String
  }

  private case class JobExecution(period: Interval[Instant], status: String, backfill: Boolean, version: String)
      extends ExecutionPeriod {
    override val aggregated: Boolean = false
  }

  private case class AggregatedJobExecution(period: Interval[Instant],
                                            completion: Double,
                                            error: Boolean,
                                            backfill: Boolean,
                                            version: String = "")
      extends ExecutionPeriod {
    override val aggregated: Boolean = true
  }

  private case class JobTimeline(jobId: String, calendarView: TimeSeriesCalendarView, executions: List[ExecutionPeriod])

  private implicit val executionPeriodEncoder = new Encoder[ExecutionPeriod] {
    override def apply(executionPeriod: ExecutionPeriod) = {
      val coreFields = List(
        "period" -> executionPeriod.period.asJson,
        "backfill" -> executionPeriod.backfill.asJson,
        "aggregated" -> executionPeriod.aggregated.asJson,
        "version" -> executionPeriod.version.asJson
      )
      val finalFields = executionPeriod match {
        case JobExecution(_, status, _, _) => ("status" -> status.asJson) :: coreFields
        case AggregatedJobExecution(_, completion, error, _, _) =>
          ("completion" -> completion.asJson) :: ("error" -> error.asJson) :: coreFields
      }
      Json.obj(finalFields: _*)
    }
  }

  private implicit val jobTimelineEncoder = new Encoder[JobTimeline] {
    override def apply(jobTimeline: JobTimeline) = jobTimeline.executions.asJson
  }

  case class ExecutionDetails(jobExecutions: Seq[ExecutionLog], parentExecutions: Seq[ExecutionLog])

  private implicit val executionDetailsEncoder: Encoder[ExecutionDetails] =
    Encoder.forProduct2("jobExecutions", "parentExecutions")(
      e => (e.jobExecutions, e.parentExecutions)
    )

  private type FocusWatchedState = ((State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeriesContext)])


  private[timeseries] def focusWatchState(): Option[FocusWatchedState] =
    Some((scheduler.state, executor.allFailingJobsWithContext))

  private[timeseries] def getFocusView(watchedState: FocusWatchedState,
                                       q: CalendarFocusQuery,
                                       filteredJobs: Set[String]): Json = {
    val startDate = Instant.parse(q.start)
    val endDate = Instant.parse(q.end)
    val period = Interval(startDate, endDate)
    val ((jobStates, backfills), _) = watchedState
    val backfillDomain =
      backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
        if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
          acc.update(Interval(bf.start, bf.end), ())
        else
          acc
      }

    val pausedJobs = scheduler.pausedJobs().map(_.id)
    val allFailingExecutionIds = executor.allFailingExecutions.map(_.id).toSet
    val allWaitingExecutionIds = executor.allRunning
      .filter(_.status == ExecutionWaiting)
      .map(_.id)
      .toSet

    def findAggregationLevel(n: Int,
                             calendarView: TimeSeriesCalendarView,
                             interval: Interval[Instant]): TimeSeriesCalendarView = {
      val aggregatedExecutions = calendarView.calendar.split(interval)
      if (aggregatedExecutions.size <= n)
        calendarView
      else
        findAggregationLevel(n, calendarView.upper(), interval)
    }

    def aggregateExecutions(
      job: TimeSeriesJob,
      period: Interval[Instant],
      calendarView: TimeSeriesCalendarView): List[(Interval[Instant], List[(Interval[Instant], JobState)])] =
      calendarView.calendar
        .split(period)
        .flatMap { interval =>
          {
            val (start, end) = interval
            val currentlyAggregatedPeriod = jobStates(job)
              .intersect(Interval(start, end))
              .toList
              .sortBy(_._1.lo)
            currentlyAggregatedPeriod match {
              case Nil => None
              case _   => Some((Interval(start, end), currentlyAggregatedPeriod))
            }
          }
        }

    def getVersionFromState(jobState: JobState): String = jobState match {
      case Done(version) => version
      case _             => ""
    }

    def getStatusLabelFromState(jobState: JobState, job: Job[TimeSeries]): String =
      jobState match {
        case Running(executionId) =>
          if (allFailingExecutionIds.contains(executionId))
            "failed"
          else if (allWaitingExecutionIds.contains(executionId))
            "waiting"
          else if (pausedJobs.contains(job.id))
            "paused"
          else "running"
        case Todo(_) => if (pausedJobs.contains(job.id)) "paused" else "todo"
        case Done(_) => "successful"
      }

    val jobTimelines =
      (for { job <- project.jobs.all if filteredJobs.contains(job.id) } yield {
        val calendarView = findAggregationLevel(
          48,
          TimeSeriesCalendarView(job.scheduling.calendar),
          period
        )
        val jobExecutions: List[Option[ExecutionPeriod]] = for {
          (interval, jobStatesOnIntervals) <- aggregateExecutions(job, period, calendarView)
        } yield {
          val inBackfill = backfills.exists(
            bf =>
              bf.jobs.contains(job) &&
                IntervalMap(interval -> 0)
                  .intersect(Interval(bf.start, bf.end))
                  .toList
                  .nonEmpty)
          if (calendarView.aggregationFactor == 1) {
            jobStatesOnIntervals match {
              case (_, state) :: Nil =>
                Some(
                  JobExecution(interval, getStatusLabelFromState(state, job), inBackfill, getVersionFromState(state)))
              case _ => None
            }
          } else {
            jobStatesOnIntervals match {
              case jobStates: List[(Interval[Instant], JobState)] if jobStates.nonEmpty => {
                val (duration, done, error) = jobStates.foldLeft((0L, 0L, false)) {
                  case ((accumulatedDuration, accumulatedDoneDuration, hasErrors), (period, jobState)) =>
                    val (lo, hi) = period.toPair
                    val jobStatus = getStatusLabelFromState(jobState, job)
                    (accumulatedDuration + lo.until(hi, ChronoUnit.SECONDS),
                     accumulatedDoneDuration + (if (jobStatus == "successful") lo.until(hi, ChronoUnit.SECONDS)
                                                else 0),
                     hasErrors || jobStatus == "failed")
                }
                Some(AggregatedJobExecution(interval, done.toDouble / duration.toDouble, error, inBackfill))
              }
              case Nil => None
            }
          }
        }
        JobTimeline(job.id, calendarView, jobExecutions.flatten)
      }).toList

    val summary =
      if (jobTimelines.isEmpty) List.empty
      else {
        jobTimelines
          .maxBy(_.executions.size)
          .calendarView
          .calendar
          .split(period)
          .flatMap {
            case (lo, hi) =>
              val isInbackfill = backfillDomain.intersect(Interval(lo, hi)).toList.nonEmpty

              case class JobSummary(periodLengthInSeconds: Long, periodDoneInSeconds: Long, hasErrors: Boolean)

              val jobSummaries: Set[JobSummary] = for {
                job <- project.jobs.all
                if filteredJobs.contains(job.id)
                (interval, jobState) <- jobStates(job).intersect(Interval(lo, hi)).toList
              } yield {
                val (lo, hi) = interval.toPair
                JobSummary(
                  periodLengthInSeconds = lo.until(hi, ChronoUnit.SECONDS),
                  periodDoneInSeconds = jobState match {
                    case Done(_) => lo.until(hi, ChronoUnit.SECONDS)
                    case _       => 0
                  },
                  hasErrors = jobState match {
                    case Running(executionId) => allFailingExecutionIds.contains(executionId)
                    case _                    => false
                  }
                )
              }
              if (jobSummaries.nonEmpty) {
                val aggregatedJobSummary = jobSummaries.reduce { (a: JobSummary, b: JobSummary) =>
                  JobSummary(a.periodLengthInSeconds + b.periodLengthInSeconds,
                            a.periodDoneInSeconds + b.periodDoneInSeconds,
                            a.hasErrors || b.hasErrors)
                }
                Some(
                  AggregatedJobExecution(
                    Interval(lo, hi),
                    aggregatedJobSummary.periodDoneInSeconds.toDouble / aggregatedJobSummary.periodLengthInSeconds.toDouble,
                    aggregatedJobSummary.hasErrors,
                    isInbackfill
                  )
                )
              } else {
                None
              }
          }

        }

    Json.obj(
      "summary" -> summary.asJson,
      "jobs" -> jobTimelines.map(jt => jt.jobId -> jt).toMap.asJson
    )
  }

  /*
    * Old part of the TimeSeriesApp
    * Can be useful so commenting now, will be deleted later
   */

  /*
  type WatchedState = ((State, Set[Backfill]), Set[(Job[TimeSeries], TimeSeriesContext)])

  def watchState(): Option[WatchedState] = Some((scheduler.state, executor.allFailingJobsWithContext))

  def getExecutions(watchedState: WatchedState): IO[Json] = {
    val job = jobs.vertices.find(_.id == jobId).get
    val calendar = job.scheduling.calendar
    val startDate = Instant.parse(start)
    val endDate = Instant.parse(end)
    val requestedInterval = Interval(startDate, endDate)
    val contextQuery = Database.sqlGetContextsBetween(Some(startDate), Some(endDate))
    val archivedExecutions =
      executor.archivedExecutions(contextQuery, Set(jobId), "", asc = true, 0, Int.MaxValue, xa)
    val runningExecutions = executor.runningExecutions
      .filter {
        case (e, _) =>
          e.job.id == jobId && e.context.toInterval.intersects(requestedInterval)
      }
      .map { case (e, status) => e.toExecutionLog(status) }

    val ((jobStates, _), _) = watchedState
    val remainingExecutions =
      for {
        (interval, maybeBackfill) <- jobStates(job)
          .intersect(requestedInterval)
          .toList
          .collect { case (itvl, Todo(maybeBackfill)) => (itvl, maybeBackfill) }
        (lo, hi) <- calendar.split(interval)
      } yield {
        val context =
          TimeSeriesContext(calendar.truncate(lo), calendar.ceil(hi), maybeBackfill, executor.projectVersion)
        ExecutionLog("", job.id, None, None, context.asJson, ExecutionTodo, None, 0)
      }
    val throttledExecutions = executor.allFailingExecutions
      .filter(e => e.job == job && e.context.toInterval.intersects(requestedInterval))
      .map(_.toExecutionLog(ExecutionThrottled))


    archivedExecutions.map(
      archivedExecutions =>
        ExecutionDetails(archivedExecutions ++ runningExecutions ++ remainingExecutions ++ throttledExecutions,
          parentExecutions(requestedInterval, job, jobStates)).asJson)
  }

  def parentExecutions(requestedInterval: Interval[Instant],
                       job: Job[TimeSeries],
                       state: Map[Job[TimeSeries], IntervalMap[Instant, JobState]]): Seq[ExecutionLog] = {

    val calendar = job.scheduling.calendar
    val parentJobs = jobs.edges
      .collect({ case (child, parent, _) if child == job => parent })
    val runningDependencies: Seq[ExecutionLog] = executor.runningExecutions
      .filter {
        case (e, _) => parentJobs.contains(e.job) && e.context.toInterval.intersects(requestedInterval)
      }
      .map({ case (e, status) => e.toExecutionLog(status) })
    val failingDependencies: Seq[ExecutionLog] = executor.allFailingExecutions
      .filter(e => parentJobs.contains(e.job) && e.context.toInterval.intersects(requestedInterval))
      .map(_.toExecutionLog(ExecutionThrottled))
    val remainingDependenciesDeps =
      for {
        parentJob <- parentJobs
        (interval, maybeBackfill) <- state(parentJob)
          .intersect(requestedInterval)
          .toList
          .collect { case (itvl, Todo(maybeBackfill)) => (itvl, maybeBackfill) }
        (lo, hi) <- calendar.split(interval)
      } yield {
        val context =
          TimeSeriesContext(calendar.truncate(lo), calendar.ceil(hi), maybeBackfill, executor.projectVersion)
        ExecutionLog("", parentJob.id, None, None, context.asJson, ExecutionTodo, None, 0)
      }

    runningDependencies ++ failingDependencies ++ remainingDependenciesDeps.toSeq
  }


*/


}
