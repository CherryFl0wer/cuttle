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
  implicit def jobAsWorkflow(job: Job[FlowScheduling]): FlowWorkflow =
    new FlowWorkflow {
      def vertices = Set(job)
      def edges = Set.empty
    }



/*  implicit def flowEncoder = new Encoder[FlowScheduling] {
    override def apply(a: FlowScheduling): Json = a.asJson
  }
*/

  implicit val routingEdgeDecoder: Decoder[RoutingKind.Routing] = deriveDecoder
  implicit val routingEdgeEncoder: Encoder[RoutingKind.Routing] = deriveEncoder

  implicit def workflowEncoder(implicit dec: Encoder[FlowJob]) =
    new Encoder[FlowWorkflow] {
      override def apply(workflow: FlowWorkflow) = {
        val jobs = workflow.vertices.map(_.id).toList.asJson
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

  implicit def workflowDecoder(implicit dec: Decoder[JobApplicable[FlowScheduling]]) =
    new Decoder[FlowWorkflow] {
      override def apply(c : HCursor): Decoder.Result[FlowWorkflow] = {
        val wf = FlowWorkflow.empty[FlowScheduling]

        for {
          jobs <- c.downField("jobs").as[List[JobApplicable[FlowScheduling]]]
          dependencies <- c.downField("dependencies").as[List[(String, String, RoutingKind.Routing)]]
          tags <- c.downField("tags").as[List[Tag]]

        } yield wf
      }
    }
}
