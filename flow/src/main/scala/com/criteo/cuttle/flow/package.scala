package com.criteo.cuttle

import scala.language.experimental.macros
import scala.language.implicitConversions
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import com.criteo.cuttle.flow.signals.EventSignal
import fs2.kafka.{Deserializer, Serializer}
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._



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




  implicit val deserializerEventSignal: Deserializer[EventSignal] = Deserializer.instance[EventSignal] {
    (_, _, bytes) =>
      import java.io.{ObjectInputStream, ByteArrayInputStream}
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject().asInstanceOf[EventSignal]
      ois.close()
      value
  }

  implicit val serializerEventSignal: Serializer[EventSignal] = Serializer.lift[EventSignal] { eventSignal =>
    import java.io.{ObjectOutputStream, ByteArrayOutputStream}
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(eventSignal)
    oos.close()
    stream.toByteArray
  }
}
