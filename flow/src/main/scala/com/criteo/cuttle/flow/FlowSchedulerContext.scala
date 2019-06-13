package com.criteo.cuttle.flow

import java.time.Instant
import java.util.{Comparator, UUID}

import com.criteo.cuttle.SchedulingContext
import doobie.free.connection.ConnectionIO
import io.circe._
import io.circe.syntax._
import cats.implicits._
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import io.circe.generic.semiauto.deriveDecoder

case class FlowSchedulerContext(start : Instant,
                                projectVersion: String = "",
                                workflowId : String,
                                resultsFromPreviousNodes : Option[Map[String, Json]] = None) extends SchedulingContext {

  // Is the result of the job
  var result : Json = Json.Null

  override def asJson: Json = FlowSchedulerContext.encoder(this)

  override def logIntoDatabase: ConnectionIO[String] = Database.serializeContext(this)

  def toId: String = s"${start}-${workflowId}-${UUID.randomUUID().toString}"

  def compareTo(other: SchedulingContext) = other match {
    case FlowSchedulerContext(timestamp, _, _, _) => start.compareTo(timestamp) // Priority compare
  }

}


// Decoder / Encoder
object FlowSchedulerContext {

  implicit val encodeInstant: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => "Instant")
  }

  private[flow] implicit val encoder: Encoder[FlowSchedulerContext] = new Encoder[FlowSchedulerContext] {
    override def apply(a: FlowSchedulerContext): Json = {
      Json.obj(
        "startAt" -> a.start.asJson,
        "workflowId" -> a.workflowId.asJson,
        "projectVersion" -> a.projectVersion.asJson,
        "cascadingResults" -> a.resultsFromPreviousNodes.asJson
      )
    }
  }
  private[flow] implicit def decoder(implicit jobs: Set[FlowJob]): Decoder[FlowSchedulerContext] = deriveDecoder

  /** Provide an implicit `Ordering` for [[FlowSchedulerContext]] based on the `compareTo` function. */
  implicit val ordering: Ordering[FlowSchedulerContext] =
    Ordering.comparatorToOrdering(new Comparator[FlowSchedulerContext] {
      def compare(o1: FlowSchedulerContext, o2: FlowSchedulerContext) = o1.compareTo(o2)
    })
}

