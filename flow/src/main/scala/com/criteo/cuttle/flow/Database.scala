package com.criteo.cuttle.flow

import com.criteo.cuttle._

import java.time._
import scala.concurrent.duration.Duration

import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.implicits._

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import doobie._
import doobie.implicits._

private[flow] object Database {
  import FlowSchedulerUtils._
  import com.criteo.cuttle.Database._


  val contextIdMigration: ConnectionIO[Unit] = {
    implicit val jobs: Set[FlowJob] = Set.empty
    val chunkSize = 1024 * 10
    val stream = sql"SELECT id, json FROM flow_contexts"
      .query[(String, Json)]
      .streamWithChunkSize(chunkSize)
    val insert = Update[(String, String)]("INSERT into tmp (id, new_id) VALUES (? , ?)")
    for {
      _ <- sql"CREATE TEMPORARY TABLE tmp (id VARCHAR(1000), new_id VARCHAR(1000))".update.run
      _ <- stream
        .chunkLimit(chunkSize)
        .evalMap { oldContexts =>
          insert.updateMany(oldContexts.map {
            case (id, json) =>
              (id, json.as[FlowSchedulerContext].right.get.toId)
          })
        }
        .compile
        .drain
      _ <- sql"""CREATE INDEX tmp_id ON tmp (id)""".update.run
      _ <- sql"""UPDATE flow_contexts ctx JOIN tmp ON ctx.id = tmp.id
                 SET ctx.id = tmp.new_id""".update.run
      _ <- sql"""UPDATE executions JOIN tmp ON executions.context_id = tmp.id
                 SET executions.context_id = tmp.new_id""".update.run
    } yield ()
  }

  val schema = List(
    sql"""
      CREATE TABLE flow_state (
        state       JSON NOT NULL,
        date        DATETIME NOT NULL
      ) ENGINE = INNODB;

      CREATE INDEX flow_state_by_date ON flow_state (date);

      CREATE TABLE flow_contexts (
        id          VARCHAR(1000) NOT NULL,
        json        JSON NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE = INNODB;
    """.update.run,
    contextIdMigration,
    NoUpdate // We removed this migration, so we reserve this slot
  )

  val doSchemaUpdates: ConnectionIO[Unit] = utils.updateSchema("flow", schema)

  def dbStateDecoder(json: Json)(implicit jobs: Set[FlowJob]): Option[State] = {
    type StoredState = List[(String, JobFlowState)]

    json.as[StoredState].right.toOption.map(_.flatMap {
      case (jobId, st) =>
        jobs.filter(_.id == jobId).map(j => j -> st)
    }.toMap)
  }

  def dbStateEncoder(state: State): Json =
    state.toList.map {
      case (job, jobState) =>
        (job.id, jobState match {
              case Done(_) => true
              case _       => false
        })
    }.asJson

  def serializeContext(context: FlowSchedulerContext): ConnectionIO[String] = {
    val id = context.toId
    sql"""
      REPLACE INTO flow_contexts (id, json)
      VALUES (
        ${id},
        ${context.asJson}
      )
    """.update.run *> Applicative[ConnectionIO].pure(id)
  }

  def deserializeState(implicit jobs: Set[FlowJob]): ConnectionIO[Option[State]] = {
    OptionT {
      sql"SELECT state FROM flow_state ORDER BY date DESC LIMIT 1"
        .query[Json]
        .option
    }.map(json => dbStateDecoder(json).get).value
  }

  def serializeState(state: State, retention: Option[Duration]): ConnectionIO[Int] = {

    val now = Instant.now()
    val cleanStateBefore = retention.map { duration =>
      if (duration.toSeconds <= 0)
        sys.error(s"State retention is badly configured: ${duration}")
      else
        now.minusSeconds(duration.toSeconds)
    }
    val stateJson = dbStateEncoder(state)

    for {
      // Apply state retention if needed
      _ <- cleanStateBefore
        .map { t =>
          sql"DELETE FROM flow_state where date < ${t}".update.run
        }
        .getOrElse(NoUpdate)
      // Insert the latest state
      x <- sql"INSERT INTO flow_state (state, date) VALUES (${stateJson}, ${now})".update.run
    } yield x
  }


}
