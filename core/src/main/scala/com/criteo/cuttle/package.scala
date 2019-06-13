package com.criteo

import scala.concurrent._
import cats.effect.IO
import cats.free._
import doobie._
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}


import cats.implicits._
import io.circe.Decoder._
import io.circe.Encoder._
import io.circe.syntax._

package cuttle {

  /**
    * Used a return type for the [[Job]] side effect instead of `Unit`.
    *
    * We use `Future[Completed]` instead of `Future[Unit]` to avoid mistakes in user code because of
    * value discarding.
    */
  sealed trait Completed

  /**
    * The object to use to successfully complete a job side effect.
    *
    * {{{
    *   Future.successful(Completed)
    * }}}
    */
  case object Finished extends Completed

}

/**
  * Core cuttle concepts are defined here.
  *
  *  - A [[CuttleProject]] is basically a [[Workflow]] to execute.
  *  - [[Workflow]]s are directed acyclic graphs of [[com.criteo.cuttle.Job]]s.
  *  - [[com.criteo.cuttle.Scheduler]] is defined for a given [[com.criteo.cuttle.Scheduling]] mechanism.
  *  - [[com.criteo.cuttle.Execution]]s are created by a [[com.criteo.cuttle.Scheduler]] for a given [[com.criteo.cuttle.Job]] and [[com.criteo.cuttle.SchedulingContext]].
  *  - [[com.criteo.cuttle.Executor]] handles the [[com.criteo.cuttle.SideEffect]]s execution.
  *  - [[com.criteo.cuttle.SideEffect]]s are plain asynchronous Scala functions and can use [[com.criteo.cuttle.ExecutionPlatform]]s to
  *    access underlying resources.
  */
package object cuttle {

  /** Doobie transactor. See https://github.com/tpolecat/doobie. */
  type XA = Transactor[IO]
  private[cuttle] val NoUpdate: ConnectionIO[Int] = Free.pure(0)

  /** The side effect function represents the real job execution. It returns a `Future[Completed]` to
    * indicate the execution result (we use [[Completed]] here instead of `Unit` to avoid automatic value
    * discarding, but [[Completed]] do not maintain additional state).
    *
    * The cuttle [[Executor]] ensures that a scheduled side effect for a given [[SchedulingContext]] will be run
    * a least once, but cannot guarantee that it will be run exactly once. That's why the side effect function must
    * be idempotent, meaning that if executed for the same [[SchedulingContext]] it must produce the same result.
    *
    * A failed future means a failed execution.
    */
  type SideEffect[S <: Scheduling] = Execution[S] => Future[Completed]

  /**
    * Automatically provide a scala `scala.concurrent.ExecutionContext` for a given [[Execution]].
    * The threadpool will be chosen carefully by the [[Executor]].
    */
  implicit def scopedExecutionContext(implicit execution: Execution[_]): ExecutionContext = execution.executionContext


  /**
  * Decoder / Encoder
  * for package cuttle core
  * */


  type JobApplicable[S <: Scheduling] = SideEffect[S] => Job[S]

  implicit val decodeTag : Decoder[Tag] = Decoder.forProduct2("name", "description")(Tag.apply)
  implicit val encodeTag : Encoder[Tag] = Encoder.forProduct2("name", "description")(t => (t.name, t.description))

  implicit val decodeJobKind : Decoder[JobKind] = io.circe.generic.semiauto.deriveDecoder[JobKind]
  implicit val encodeJobKind : Encoder[JobKind] = io.circe.generic.semiauto.deriveEncoder[JobKind]

  implicit def jobDecoder[S <: Scheduling : Decoder] =
    new Decoder[JobApplicable[S]] {
      override def apply(cursor : HCursor) : Result[JobApplicable[S]] = for {
        id   <- cursor.downField("id").as[String]
        kind <- cursor.downField("kind").as[JobKind]
        description <- cursor.downField("description").as[String]
        tags <- cursor.downField("tags").as[Set[String]]
        scheduling <- cursor.downField("scheduling").as[S]
      } yield Job[S](id, description, scheduling, kind, tags.map(t => Tag(t))) _

  }

  implicit def jobEncoder[S <: Scheduling : Encoder] =
    new Encoder[Job[S]] {
      override def apply(job: Job[S]) : Json = {
        Json.obj(
          "id" -> job.id.asJson,
          "kind" -> job.kind.asJson,
          "description" -> Option(job.description).filterNot(_.isEmpty).getOrElse("No description").asJson,
          "scheduling" -> job.scheduling.asJson,
          "tags" -> job.tags.map(_.name).asJson
        )
    }
  }

}
