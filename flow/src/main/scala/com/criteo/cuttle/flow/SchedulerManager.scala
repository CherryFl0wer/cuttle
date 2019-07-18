package com.criteo.cuttle.flow

import cats.effect.{Concurrent, IO}
import com.criteo.cuttle.{ExecutionPlatform, RetryStrategy, XA}
import fs2.Stream
import fs2.concurrent.Queue

import scala.concurrent.duration.Duration

class SchedulerManager(workflowToRun : Queue[IO, SchedulerManager.QueueData], transactorQuery : XA) {

  def push(graph : FlowGraph): IO[Unit] = for {
    _ <- workflowToRun.enqueue1((graph, FlowGraph.defaultPlatforms, None, None))
  } yield ()

  def push(graph : FlowGraph,
           platforms: Seq[ExecutionPlatform],
           retryStrategy: Option[RetryStrategy] = None,
           logsRetention: Option[Duration] = None): IO[Unit] = for {
    _ <- workflowToRun.enqueue1((graph, platforms, retryStrategy, logsRetention))
  } yield ()


  def run()(implicit C : Concurrent[IO]) =
    workflowToRun
      .dequeue
      .map { case (project, platform, strategy, logs) =>
        project.start(transactorQuery, platform, strategy, logs)
      }
      .parJoin(5)
}

object SchedulerManager {

  import com.criteo.cuttle.flow.{Database => FlowDB}
  import com.criteo.cuttle.{DatabaseConfig, Logger, Database => CoreDB}
  import doobie.implicits._

  type QueueData = (FlowGraph, Seq[ExecutionPlatform], Option[RetryStrategy], Option[Duration])

  /**
     Create a new scheduler manager
    * @param maxWorkflow
    * @param dbConfig
    * @param F
    * @param logger
    * @return
    */
  def apply(maxWorkflow : Int, dbConfig : DatabaseConfig = DatabaseConfig.fromEnv)(implicit F : Concurrent[IO], logger : Logger) = {
    val xa = CoreDB.connect(dbConfig)(logger)

    logger.info("Applying migrations to database")

    for {
      _ <- Stream.eval(FlowDB.doSchemaUpdates.transact(xa))
      _ = logger.info("Database up-to-date")
      queue <- Stream.eval(Queue.bounded[IO, QueueData](maxWorkflow))
    } yield new SchedulerManager(queue, xa)
  }
}

/*
  sm <- SchedulerManager(100)

  run <- Stream(
    sm.run.parJoinUnbounded,
    signalManager.consumerKafka.drain
  )
*/