package com.criteo.cuttle.flow

import com.criteo.cuttle._
import java.time._

import scala.concurrent.duration.Duration
import cats.Applicative
import cats.data.{EitherT, OptionT}
import cats.implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import doobie._
import doobie.implicits._
import com.criteo.cuttle.{utils => CriteoCoreUtils}

private[flow] object Database {
  import FlowSchedulerUtils._
  import com.criteo.cuttle.Database._


  lazy val contextIdMigration: ConnectionIO[Unit] = {
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
      _ <-
        sql"""UPDATE flow_contexts
              SET id = tmp.new_id
              FROM tmp WHERE flow_contexts.id = tmp.id
        """.update.run
      _ <- sql"""UPDATE executions
                 SET context_id = tmp.new_id
                 FROM tmp WHERE executions.context_id = tmp.id
           """.update.run
    } yield ()
  }

  /*
  * Schema describing the different tables necessary to run FlowWorkflow
  *
  * */
  val schema = List(
    sql"""
      CREATE TABLE flow_state (
        workflow_id  TEXT NOT NULL,
        state       JSONB NOT NULL,
        date        TIMESTAMP WITHOUT TIME ZONE NOT NULL
      );
      CREATE INDEX flow_state_workflowid ON flow_state (workflow_id);
      CREATE INDEX flow_state_by_date ON flow_state (date);

      CREATE TABLE flow_results (
        workflow_id  TEXT NOT NULL,
        from_graph   VARCHAR(255) NOT NULL,
        step_id  TEXT NOT NULL,
        inputs JSONB NULL,
        result JSONB NULL,
        PRIMARY KEY(workflow_id, step_id)
      );
      CREATE INDEX flow_results_workflowid ON flow_results (workflow_id);
      CREATE INDEX flow_results_stepid ON flow_results (step_id);

      CREATE TABLE flow_contexts (
        id          TEXT NOT NULL,
        json        JSONB NOT NULL,
        PRIMARY KEY (id)
      );

      CREATE TABLE flow_graph (
         id          TEXT NOT NULL,
         json        JSONB NOT NULL,
         PRIMARY KEY (id)
      );
      CREATE INDEX flow_graph_id ON flow_graph (id);
    """.update.run,
    contextIdMigration,
    NoUpdate
  )

  val doSchemaUpdates: ConnectionIO[Unit] = CriteoCoreUtils.updateSchema("flow", schema)


  def insertResult(wfHash : String, wfId : String, stepId : String, inputs : Json, result : Json) =
    sql"""
          INSERT INTO flow_results (workflow_id, from_graph, step_id, inputs, result)
          VALUES(${wfId}, ${wfHash}, ${stepId}, ${inputs}, ${result})
      """.update.run

  // TODO : Maybe return list or one response.
  def retrieveResult(wfId : String, stepId : String) =
    OptionT {
      sql"""
            SELECT result FROM flow_results WHERE workflow_id = ${wfId} AND step_id = ${stepId} ORDER BY date LIMIT 1
    """.query[Json].option
    }.value

  /**
    * Decode the state
    * @param json
    * @param jobs
    * @return a potential state
    */
  def dbStateDecoder(json: Json)(implicit jobs: Set[FlowJob]): Option[JobState] = {
    type StoredState = List[(String, JobFlowState)]
    val stored = json.as[StoredState]
    stored.right.toOption.flatMap { jobsStored =>
        val state = jobsStored.map { case (jobId, flowState) =>
           jobs.find(_.id == jobId).map(job => job -> flowState)
        }

        Some(state.flatten.toMap)
    }
  }

  /**
    * Encode the state
    * @param state
    * @return
    */
  def dbStateEncoder(state: JobState): Json = state.toList.map {
      case (job, flowState) =>  (job.id, flowState.asJson)
    }.asJson

  def deserializeState(workflowId: String)(implicit jobs: Set[FlowJob]): ConnectionIO[Option[JobState]] = {
    OptionT {
      sql"SELECT state FROM flow_state WHERE workflow_id = ${workflowId} ORDER BY date DESC LIMIT 1"
        .query[Json]
        .option
    }.map(json => dbStateDecoder(json).get).value
  }

  def serializeState(workflowid : String, state: JobState, retention: Option[Duration]): ConnectionIO[Int] = {

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
      x <- sql"INSERT INTO flow_state (workflow_id, state, date) VALUES (${workflowid}, ${stateJson}, ${now})".update.run
    } yield x

  }


  /**
    * Serialize context
    * @param context
    * @return the id of the context
    */
  def serializeContext(context: FlowSchedulerContext): ConnectionIO[String] = {
    val id = context.toId
    sql"""INSERT INTO flow_contexts(id, json)
          VALUES(${id}, ${context.asJson})
          ON CONFLICT (id) DO UPDATE
          SET json = excluded.json
    """.update.run *> Applicative[ConnectionIO].pure(id)
  }

  /**
    * Serialize the graph into the db
    * @param workflow
    * @return the hash of the workflow
    */
  def serializeGraph(workflow: FlowWorkflow) = {

    val h = workflow.hash.toString
    val wf = workflow.asJson
    for {
      exist <- EitherT(sql"""SELECT exists(SELECT 1 FROM flow_graph WHERE id=${h})""".query[Boolean].option.attempt)
      _ <- if (exist.isEmpty || !exist.get)
        EitherT(sql"""INSERT INTO flow_graph VALUES(${h}, ${wf})""".update.run.attempt)
      else
        EitherT.rightT[doobie.ConnectionIO, Throwable](())
    } yield ()

  }

}
