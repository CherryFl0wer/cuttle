package com.criteo.cuttle.flow

import com.criteo.cuttle.Scheduling
import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._

/**
  * A [[FlowScheduling]] has a role of defining the configuration of the scheduler
  *
  */

abstract class FlowArg
final case class NoArg() extends FlowArg

case class FlowScheduling[+Input <: FlowArg : Encoder : Decoder, +Output <: FlowArg : Encoder : Decoder](input : Input, output : Output)
  extends Scheduling {

  type Context = FlowSchedulerContext

  override def asJson: Json = Json.obj(
    "kind" -> "flow".asJson
  )
}