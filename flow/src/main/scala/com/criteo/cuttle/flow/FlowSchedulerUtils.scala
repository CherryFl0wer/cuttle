package com.criteo.cuttle.flow

import java.time.ZoneId

import com.criteo.cuttle.{Completed, Job}
import io.circe.Json

import scala.concurrent.Future

object FlowSchedulerUtils {
  type FlowJob = Job[FlowScheduling[FlowArg, FlowArg]]

  type Executable = (FlowJob, FlowSchedulerContext) // A job to be executed
  type RunJob = (FlowJob, FlowSchedulerContext, Future[Completed]) // Job, Context, Result
  type JobState = Map[FlowJob, JobFlowState]
  type JobResults = Map[FlowJob, Json]

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


  import fs2.{Pipe, Pull, Stream}
  /***
    * Execute `call` each time there is a new value inside the stream
    * @param call Every time a result is added to the stream then execute this function that consume the last element
    *             of the stream
    * @param over Predicate, if return true then stop trampoline otherwise keep going
    * @return the stream with the different result trampolined
    */
  def trampoline[F[_], O](call : O => F[O], over : O => Boolean) : Pipe[F, O, O] = {

    def go(s: Stream[F, O]): Pull[F, O, Unit] = {
      s.pull.uncons.flatMap {
        case Some((hd,tl)) =>
          hd.size match {
            case m if m >= 1 => { // Whatever the size is, we want the head
              val elm = hd.head.get
              Pull.output(hd.take(1)) >> (if (over(elm)) Pull.done else go(Stream.eval(call(elm)))
                )

            }
            case 0 => Pull.done // No element in chunks then done
          }
        case None => Pull.done
      }
    }

    in => go(in).stream
  }

}
