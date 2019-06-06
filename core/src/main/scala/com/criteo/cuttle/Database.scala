package com.criteo.cuttle

import java.time._
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration => ScalaDuration}
import doobie._
import doobie.implicits._
import doobie.hikari._
import io.circe.syntax._
import io.circe.Json
import io.circe.parser._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.{IO, Resource}
import doobie.util.log
import com.criteo.cuttle.events.{Event, JobSuccessForced}
import org.postgresql.util.PGobject

/** Configuration of JDBC endpoint.
  *
  * @param host JDBC driver host
  * @param port JDBC driver port
  */
case class DBLocation(host: String, port: Int)

/** Configuration for the Postgresql database used by Cuttle.
  *
  * @param locations sequence of JDBC endpoints
  * @param database JDBC database
  * @param username JDBC username
  * @param password JDBC password
  */
case class DatabaseConfig(locations: Seq[DBLocation], database: String, username: String, password: String)

/** Utilities for [[DatabaseConfig]]. */
object DatabaseConfig {

  /** Creates a [[DatabaseConfig]] instance */
  def fromEnv: DatabaseConfig = {
    def env(variable: String, default: Option[String] = None) =
      Option(System.getenv(variable)).orElse(default).getOrElse(s"Missing env ${'$' + variable}")

    val host = env("DB_HOST", Some("localhost"))
    val port = env("DB_PORT", Some("5532"))
    val dbname = env("DB_NAME", Some("audience"))
    val user = env("DB_USER", Some("audience"))
    val pwd  = env("DB_PASSWORD", Some("password"))
    //TODO
    DatabaseConfig(Seq(DBLocation(host, port.toInt)), dbname, user, pwd)
  }
}

private[cuttle] object Database {


  // From doobie doc
  implicit val jsonMeta: Meta[Json] = Meta.Advanced.other[PGobject]("json").timap[Json](
      a => parse(a.getValue).leftMap[Json](e => throw e).merge)(
      a => {
        val o = new PGobject
        o.setType("json")
        o.setValue(a.noSpaces)
        o
      }
    )

  val schemaEvolutions = List(
    sql"""
      CREATE TABLE executions (
        id          CHAR(36) NOT NULL,
        job         TEXT NOT NULL,
        start_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        end_time    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        context_id  TEXT NOT NULL,
        success     BOOLEAN NOT NULL,
        waiting_seconds INT NOT NULL,
        PRIMARY KEY (id)
      );

      CREATE INDEX execution_by_context_id ON executions (context_id);
      CREATE INDEX execution_by_job ON executions (job);
      CREATE INDEX execution_by_start_time ON executions (start_time);

      CREATE TABLE paused_jobs (
        id          TEXT NOT NULL,
        PRIMARY KEY (id)
      );

      CREATE TABLE executions_streams (
        id          CHAR(36) NOT NULL,
        streams     TEXT
      );
    """.update.run,
    sql"""
      ALTER TABLE paused_jobs ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT '1991-11-01:15:42:00'
    """.update.run,
    sql"""
      ALTER TABLE executions_streams ADD PRIMARY KEY(id)
    """.update.run,
    sql"""
      CREATE TABLE events (
        created      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        kind         VARCHAR(100),
        job_id       TEXT,
        execution_id CHAR(36),
        start_time   TIMESTAMP WITHOUT TIME ZONE,
        end_time     TIMESTAMP WITHOUT TIME ZONE,
        payload      TEXT NOT NULL
      );

      CREATE INDEX events_by_created ON events (created);
      CREATE INDEX events_by_job_id ON events (job_id);
      CREATE INDEX events_by_execution_id ON events (execution_id);
      CREATE INDEX events_by_start_time ON events (start_time);
      CREATE INDEX events_by_end_time ON events (end_time);
    """.update.run
  )

  private def lockedTransactor(xa: Transactor[IO], releaseIO: IO[Unit]): Transactor[IO] = {
    val guid = java.util.UUID.randomUUID.toString

    // Try to insert our lock at bootstrap
    (for {
      _ <- sql"""CREATE TABLE IF NOT EXISTS locks (
          locked_by       VARCHAR(36) NOT NULL,
          locked_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL
        );
      """.update.run
      locks <- sql"""
          SELECT locked_by, locked_at FROM locks WHERE floor((EXTRACT(EPOCH FROM locked_at) - EXTRACT(EPOCH FROM NOW()))/60) < 5
        """.query[(String, Instant)].to[List]
      _ <- if (locks.isEmpty) {
        sql"""
            DELETE FROM locks;
            INSERT INTO locks VALUES (${guid}, NOW());
          """.update.run
      } else {
        sys.error(s"Database already locked: ${locks.head}")
      }
    } yield ()).transact(xa).unsafeRunSync

    // Remove lock on shutdown
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run =
        sql"""
          DELETE FROM locks WHERE locked_by = $guid;
        """.update.run.transact(xa).unsafeRunSync
    })

    // Refresh our lock every minute (and check that we still are the lock owner)
    ThreadPools
      .newScheduledThreadPool(1, poolName = Some("DatabaseLock"))
      .scheduleAtFixedRate(
        new Runnable {
          def run =
            if ((sql"""
            UPDATE locks SET locked_at = NOW() WHERE locked_by = ${guid}
          """.update.run.transact(xa).unsafeRunSync: Int) != 1) {
              releaseIO.unsafeRunSync()
              sys.error(s"Lock has been lost, shutting down the database connection.")
            }
        },
        1,
        1,
        TimeUnit.MINUTES
      )

    // We can now use the transactor safely
    xa
  }

  private[cuttle] val doSchemaUpdates = utils.updateSchema("schema_evolutions", schemaEvolutions)

  private val connections = collection.concurrent.TrieMap.empty[DatabaseConfig, XA]

  private[cuttle] def newHikariTransactor(dbConfig: DatabaseConfig): Resource[IO, HikariTransactor[IO]] = {
    import com.criteo.cuttle.ThreadPools.Implicits.doobieContextShift

    val locationString = dbConfig.locations.map(dbLocation => s"${dbLocation.host}:${dbLocation.port}").mkString(",")
    val jdbcString = s"jdbc:postgresql://$locationString/${dbConfig.database}" +
      "?serverTimezone=UTC&useSSL=false&allowMultiQueries=true&failOverReadOnly=false&rewriteBatchedStatements=true"

    for {
      connectThreadPool <- ThreadPools.doobieConnectThreadPoolResource
      transactThreadPool <- ThreadPools.doobieTransactThreadPoolResource
      transactor <- HikariTransactor.newHikariTransactor[IO](
        "org.postgresql.Driver",
        jdbcString,
        dbConfig.username,
        dbConfig.password,
        connectThreadPool,
        transactThreadPool
      )
    } yield transactor
  }

  // we remove all created Hikari transactors here,
  // it can be handy if you to recreate a connection with same DbConfig
  private[cuttle] def reset(): Unit =
    connections.clear()

  def connect(dbConfig: DatabaseConfig)(implicit logger: Logger): XA = {
    // FIXME we shouldn't use allocated as it's unsafe instead we have to flatMap on the Resource[HikariTransactor]

    val (hirakiTransactor, releaseIO) = newHikariTransactor(dbConfig).allocated.unsafeRunSync()

    logger.debug("Allocated new Hikari transactor")
    connections.getOrElseUpdate(
      dbConfig, {
        val xa = lockedTransactor(hirakiTransactor, releaseIO)
        logger.debug("Lock transactor")
        doSchemaUpdates.transact(xa).unsafeRunSync
        logger.debug("Update Cuttle Schema")
        xa
      }
    )
  }

}

private[cuttle] case class Queries(logger: Logger) {

  implicit val logHandler: log.LogHandler = DoobieLogsHandler(logger).handler

  def logExecution(e: ExecutionLog, logContext: ConnectionIO[String]): ConnectionIO[Int] =
    for {
      contextId <- logContext
      result <- sql"""
        INSERT INTO executions (id, job, start_time, end_time, success, context_id, waiting_seconds)
        VALUES (${e.id}, ${e.job}, ${e.startTime}, ${e.endTime}, ${e.status}, ${contextId}, ${e.waitingSeconds})
        """.update.run
    } yield result

  def getExecutionLogSize(jobs: Set[String]): ConnectionIO[Int] =
    (sql"""
      SELECT COUNT(*) FROM executions WHERE """ ++ Fragments.in(fr"job", NonEmptyList.fromListUnsafe(jobs.toList)))
      .query[Int]
      .unique

  private[cuttle] def orderBy(sort: String, asc: Boolean) = (sort, asc) match {
    case ("context", true)    => fr"ORDER BY context_id ASC, job, id"
    case ("context", false)   => fr"ORDER BY context_id DESC, job, id"
    case ("job", true)        => fr"ORDER BY job ASC, context_id, id"
    case ("job", false)       => fr"ORDER BY job DESC, context_id, id"
    case ("status", true)     => fr"ORDER BY success ASC, context_id, job, id"
    case ("status", false)    => fr"ORDER BY success DESC, context_id, job, id"
    case ("startTime", true)  => fr"ORDER BY start_time ASC, id"
    case ("startTime", false) => fr"ORDER BY start_time DESC, id"
    case (_, true)            => fr"ORDER BY end_time ASC, id"
    case _                    => fr"ORDER BY end_time DESC, id"
  }

  private[cuttle] def whereJobIn(jobs: Set[String]) =
    if (jobs.isEmpty)
      Fragment.empty
    else
      Fragments.in(fr"WHERE job", NonEmptyList.fromListUnsafe(jobs.toList))

  private[cuttle] def pagination(limit: Int, offset: Int) = fr"LIMIT $limit OFFSET $offset"

  private def query(sql: Fragment) =
    sql
      .query[(String, String, Instant, Instant, String, ExecutionStatus, Int)] // OC : Json now String
      .to[List]
      .map(_.map {
        case (id, job, startTime, endTime, context, status, waitingSeconds) =>
          ExecutionLog(id, job, Some(startTime), Some(endTime), context.asJson, status, waitingSeconds = waitingSeconds)
      })

  def getExecutionLog(contextQuery: Fragment,
                      jobs: Set[String],
                      sort: String,
                      asc: Boolean,
                      offset: Int,
                      limit: Int): ConnectionIO[List[ExecutionLog]] = {
    val select =
      fr"""
        SELECT executions.id, job, start_time, end_time, contexts.json AS context, success, executions.waiting_seconds
        FROM executions
      """

    val context = fr"INNER JOIN (" ++ contextQuery ++ fr") contexts ON executions.context_id = contexts.id"

    val finalQuery =
      select ++
        context ++
        whereJobIn(jobs) ++
        orderBy(sort, asc) ++
        pagination(limit, offset)

    query(finalQuery)
  }

  /**
    * Used to query archived executions without a context.
    */
  def getRawExecutionLog(jobs: Set[String],
                         sort: String,
                         asc: Boolean,
                         offset: Int,
                         limit: Int): ConnectionIO[List[ExecutionLog]] = {
    val select =
      fr"""
        SELECT id, job, start_time, end_time, context_id AS context, success, executions.waiting_seconds
        FROM executions
      """
    val finalQuery =
      select ++
        whereJobIn(jobs) ++
        orderBy(sort, asc) ++
        pagination(limit, offset)

    query(finalQuery)
  }

  def getExecutionById(contextQuery: Fragment, id: String): ConnectionIO[Option[ExecutionLog]] =
    (sql"""
      SELECT executions.id, job, start_time, end_time, contexts.json AS context, success, executions.waiting_seconds
      FROM executions INNER JOIN (""" ++ contextQuery ++ sql""") contexts
      ON executions.context_id = contexts.id WHERE executions.id = $id""")
      .query[(String, String, Instant, Instant, String, ExecutionStatus, Int)] // OC : Json now String
      .option
      .map(_.map {
        case (id, job, startTime, endTime, context, status, waitingSeconds) =>
          ExecutionLog(id, job, Some(startTime), Some(endTime), context.asJson, status, waitingSeconds = waitingSeconds)
      })

  def resumeJob(id: String): ConnectionIO[Int] =
    sql"""
      DELETE FROM paused_jobs WHERE id = $id
    """.update.run

  private[cuttle] def pauseJobQuery[S <: Scheduling](pausedJob: PausedJob) = sql"""
      INSERT INTO paused_jobs VALUES (${pausedJob.id}, ${pausedJob.date})
    """.update

  def pauseJob(pausedJob: PausedJob): ConnectionIO[Int] =
    resumeJob(pausedJob.id) *> pauseJobQuery(pausedJob).run

  private[cuttle] val getPausedJobIdsQuery = sql"SELECT id, date FROM paused_jobs".query[PausedJob]

  def getPausedJobs: ConnectionIO[Seq[PausedJob]] = getPausedJobIdsQuery.to[Seq]

  def archiveStreams(id: String, streams: String): ConnectionIO[Int] =
    sql"""
      INSERT INTO executions_streams (id, streams) VALUES
      (${id}, ${streams})
    """.update.run

  def applyLogsRetention(logsRetention: ScalaDuration): ConnectionIO[Int] = // TODO : Need test because switched to PG format request
    sql"""
          DELETE FROM executions_streams AS es USING executions AS e
          WHERE es.id = e.id AND end_time < ${Instant.now.minusSeconds(logsRetention.toSeconds)}
    """.update.run

  def archivedStreams(id: String): ConnectionIO[Option[String]] =
    sql"SELECT streams FROM executions_streams WHERE id = ${id}"
      .query[String]
      .option

  def jobStatsForLastThirtyDays(jobId: String): ConnectionIO[List[ExecutionStat]] =
    sql"""
         select
             start_time,
             end_time,
             floor((EXTRACT(EPOCH FROM start_time) - EXTRACT(EPOCH FROM end_time))) as duration_seconds,
             waiting_seconds as waiting_seconds,
             success
         from executions
         where job=$jobId and end_time > (NOW() - INTERVAL '30' DAY) order by start_time asc, end_time asc
       """
      .query[(Instant, Instant, Int, Int, ExecutionStatus)]
      .to[List]
      .map(_.map {
        case (startTime, endTime, durationSeconds, waitingSeconds, status) =>
          new ExecutionStat(startTime, endTime, durationSeconds, waitingSeconds, status)
      })

  def logEvent(e: Event): ConnectionIO[Int] = {
    val payload = e.asJson.toString() // OC : Json now String
    val query: Update0 = e match {
      case JobSuccessForced(date, job, start, end) =>
        sql"""INSERT INTO events (created, kind, job_id, start_time, end_time, payload)
              VALUES (${date},  ${e.getClass.getSimpleName}, ${job}, ${start}, ${end}, ${payload})""".update
    }
    query.run
  }

  val healthCheck: ConnectionIO[Boolean] =
    sql"""select 1 from dual"""
      .query[Boolean]
      .unique
}

object Queries {
  val getAllContexts: Fragment =
    sql"""
      SELECT context_id as id, context_id as json FROM executions
    """
}
