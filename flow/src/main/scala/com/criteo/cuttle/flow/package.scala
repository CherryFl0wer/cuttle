package com.criteo.cuttle

import scala.language.experimental.macros
import scala.language.implicitConversions
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._
import cats.syntax.either._



package object flow {

  import io.circe.generic.semiauto.{deriveEncoder, deriveDecoder}
  /** Convert a single job to Workflow of a single job. */
  implicit def jobAsWorkflow(job: FlowJob) =
    new FlowWorkflow {
      def vertices = Set(job)
      def edges = Set.empty
    }

  implicit val routingEdgeDecoder: Decoder[RoutingKind.Routing] = deriveDecoder
  implicit val routingEdgeEncoder: Encoder[RoutingKind.Routing] = deriveEncoder

  implicit val NoArgDecoder: Decoder[NoArg] = io.circe.generic.semiauto.deriveDecoder[NoArg]
  implicit val NoArgEncoder: Encoder[NoArg] = io.circe.generic.semiauto.deriveEncoder[NoArg]

  implicit def workflowEncoder(implicit enc: Encoder[FlowJob]) =
    new Encoder[FlowWorkflow] {
      override def apply(workflow: FlowWorkflow) = {
        val jobs = workflow.vertices.map(_.id).asJson
        val tags = workflow.vertices.flatMap(_.tags).asJson
        val dependencies = workflow.edges.map {
          case (from, to, kind) =>
            Json.obj(
              "from" -> from.id.asJson,
              "to" -> to.id.asJson,
              "kind" -> kind.toString.asJson
            )
        }.asJson

        Json.obj(
          "jobs" -> jobs,
          "dependencies" -> dependencies,
          "tags" -> tags
        )
      }
    }
}
