package com.criteo.cuttle.flow

import com.criteo.cuttle.Scheduling
import io.circe.Json
import io.circe.syntax._

/**
  * A [[FlowScheduling]] has a role of defining the configuration of the scheduler
  *
  * @param inputs Supposed to be the params that can be used inside a job
  *               Currently defined by a string containing a parsable json object
  */
case class FlowScheduling(inputs : Json = Json.Null, outputs : Json = Json.Null) extends Scheduling {

  type Context = FlowSchedulerContext

  override def equals(obj: Any): Boolean = obj match {
    case FlowScheduling(_, _) => true
    case _ => false
  }

  override def asJson: Json = Json.obj(
    "kind" -> "flow".asJson,
    "inputs" -> inputs,
    "outputs" -> outputs
  )
}