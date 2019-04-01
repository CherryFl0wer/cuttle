package com.criteo.cuttle

import scala.language.experimental.macros
import scala.language.implicitConversions

import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob

import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._

import cats.syntax.either._


package object flow {


  /** Defines an implicit default dependency job kind for [[FlowScheduling]] graphs
    * Must be available during DSL
    * */
  implicit val defaultDependencyDescriptor: JobKind = Success

  /** Convert a single job to Workflow of a single job. */
  implicit def jobAsWorkflow(job: Job[FlowScheduling]): FlowWorkflow =
    new FlowWorkflow {
      def vertices = Set(job)
      def edges = Set.empty
    }



  implicit def flowEncoder = new Encoder[FlowScheduling] {
    override def apply(a: FlowScheduling): Json = a.asJson
  }



  implicit def flowWFEncoder(implicit dec: Encoder[FlowJob]) =
    new Encoder[FlowWorkflow] {
      override def apply(workflow: FlowWorkflow) = {
        val jobs = workflow.jobsInOrder.asJson
        val tags = workflow.vertices.flatMap(_.tags).asJson
        val dependencies = workflow.edges.map {
          case (to, from, _) =>
            Json.obj(
              "from" -> from.id.asJson,
              "to" -> to.id.asJson
            )
        }.asJson

        Json.obj(
          "jobs" -> jobs,
          "dependencies" -> dependencies,
          "tags" -> tags
        )
      }
    }

  implicit def flowWFDecoder(implicit dec: Decoder[JobApplicable[FlowScheduling]]) =
    new Decoder[FlowWorkflow] {

      // Might move this
      case class Edge(from : String, to : String)

      // Same
      implicit val decodeEdge : Decoder[Edge] = Decoder.forProduct2("from", "to")(Edge.apply)


      override def apply(c : HCursor): Decoder.Result[FlowWorkflow] = {
        val wf = FlowWorkflow.empty[FlowScheduling]

        for {
          jobs <- c.downField("jobs").as[List[JobApplicable[FlowScheduling]]]
          dependencies <- c.downField("dependencies").as[List[Edge]]
          tags <- c.downField("tags").as[List[Tag]]

        } yield wf
      }
    }
}
