package com.criteo.cuttle.flow

import java.time.ZoneId

import com.criteo.cuttle.flow.signals._
import com.criteo.cuttle.{Completed, Job}
import io.circe.{Json, JsonObject}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object FlowSchedulerUtils {


  type WFSignalBuilder[K,V] = SignalManager[K,V] => FlowWorkflow

  type FlowJob = Job[FlowScheduling]

  type Executable = (FlowJob, FlowSchedulerContext) // A job to be executed
  type RunJob = (FlowJob, FlowSchedulerContext, Future[Completed]) // Job, Context, Result
  type JobState = Map[FlowJob, JobFlowState]

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



  /**
  Format every key of the given json by using the formatFunc.
    * @param json The json to format
    * @param formatFunc The function to transform the key
    * @return the new json
    */
  def formatKeyOnJson(json : Json, formatFunc : String => String) : Json = {
    @scala.annotation.tailrec
    def format(jsonAcc : JsonObject, jsonContent : List[(String, Json)]) : Json = {
      jsonContent match {
        case (key, value) :: tail =>
          if (value.isObject) {
            format(jsonAcc.add(formatFunc(key), formatKeyOnJson(value, formatFunc)), tail)
          } else format(jsonAcc.add(formatFunc(key), value), tail)
        case  Nil => Json.fromJsonObject(jsonAcc)
      }
    }
    json.asObject.fold(Json.Null)(jsObj => format(JsonObject.empty, jsObj.toList))
  }



  /**
    Merge jsons together, if there is two key similar at the first level then create a map where
    the keys are nexts._1
    * @param initial initial json
    * @param nexts list of others json, tuple containing a string defining the name of the json and a Json attached to it
    * @return
    */
  def mergeDuplicateJson(initial : (String, Json), nexts : List[(String, Json)]) = {
    import io.circe.Json.fromJsonObject
    import io.circe.syntax._

    val mergedKeys : ListBuffer[String] = ListBuffer.empty
    nexts.fold(initial) {
      case (accInit, (rightJsonName, rightJsonVal)) =>
        (accInit._1,
          (accInit._2.asObject, rightJsonVal.asObject) match {
            case (Some(lhs), Some(rhs)) => {
              fromJsonObject(rhs.toList.foldLeft(lhs) {
                case (leftJson, (rkey, rvalue)) => // traversal over right json
                  lhs(rkey).fold(leftJson.add(rkey, rvalue)) { leftValJson => // If right key exist in left json
                    if (mergedKeys.contains(rkey)) {
                      leftJson.remove(rkey)
                      val mapValue = leftValJson.asObject.get.toMap ++ Map(rightJsonName -> rvalue)
                      leftJson.add(rkey, mapValue.asJson)
                    } else {
                      mergedKeys.append(rkey)
                      leftJson.remove(rkey)
                      leftJson.add(rkey, Json.obj(
                        accInit._1 -> leftValJson,
                        rightJsonName -> rvalue
                      ))
                    }
                  }

              })
            }
            case (None, Some(rhs)) => fromJsonObject(rhs)
            case (Some(lhs), None) => fromJsonObject(lhs)
            case _ => accInit._2
          })
    }
  }


  import fs2.{Pipe, Pull, Stream}
  /**
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
              Pull.output(hd.take(1)) >> (if (over(elm)) Pull.done else go(Stream.eval(call(elm))))
            }
            case 0 => Pull.done // No element in chunks then done
          }
        case None => Pull.done
      }
    }

    in => go(in).stream
  }

}
