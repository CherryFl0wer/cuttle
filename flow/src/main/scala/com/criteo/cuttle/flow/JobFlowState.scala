package com.criteo.cuttle.flow

import cats._
import cats.implicits._
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._

private[flow] sealed trait JobFlowState
private[flow] case object Done extends JobFlowState
private[flow] case class Running(executionId: String) extends JobFlowState
private[flow] case object Failed extends JobFlowState

private[flow] object JobFlowState {
  implicit val encoder: Encoder[JobFlowState] = deriveEncoder
  implicit def decoder(implicit jobs: Set[JobFlowState]): Decoder[JobFlowState] = deriveDecoder
  implicit val eqInstance: Eq[JobFlowState] = Eq.fromUniversalEquals[JobFlowState]
}