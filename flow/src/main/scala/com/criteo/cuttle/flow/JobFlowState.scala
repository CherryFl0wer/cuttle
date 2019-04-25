package com.criteo.cuttle.flow

import cats._
import cats.implicits._
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._

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