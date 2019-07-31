package com.criteo.cuttle.flow

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, IO}
import com.criteo.cuttle
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import com.criteo.cuttle.flow.signals.SignalManager
import com.criteo.cuttle.{Completed, ExecutionPlatform, RetryStrategy, XA}
import fs2.Stream
import fs2.concurrent.Queue

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class WorkflowsManager(workflowToRun : Queue[IO, WorkflowsManager.QueueData],
                       transactorQuery : XA,
                       signalManager : SignalManager[String, _])(semaphore: Semaphore[IO]) {


  /**
    * Add a graph to the workflow manager and create a new topic inside the signal manager
    * @param graph The graph
    * @param platforms The configured [[ExecutionPlatform ExecutionPlatforms]] to use to execute jobs.
    * @param retryStrategy strategy to use for execution retry
    * @param logsRetention clean the execution logs older than the given duration
    * @return
    */
  def push(graph : FlowCreator,
           platforms: Seq[ExecutionPlatform] = FlowCreator.defaultPlatforms,
           retryStrategy: Option[RetryStrategy] = None,
           logsRetention: Option[Duration] = None): IO[Unit] = for {
    _ <- signalManager.newTopic(graph.workflowId)
    _ <- workflowToRun.enqueue1((graph, platforms, retryStrategy, logsRetention))
  } yield ()

  def run(parallelRun : Int)(implicit C : Concurrent[IO]):
  Stream[IO, List[Either[Throwable, Set[(FlowJob, FlowSchedulerContext, Future[Completed])]]]] =
    workflowToRun
      .dequeue
      .parEvalMap(parallelRun) {
        case (project, platform, strategy, logs) =>
          project
              .parStart(transactorQuery, semaphore, platform, strategy, logs)
              .compile
              .toList
              .flatMap(
                lst => signalManager
                  .removeTopic(project.workflowId)
                  .map(_ => lst)
              )
      }

}

object WorkflowsManager {

  import com.criteo.cuttle.flow.{Database => FlowDB}
  import com.criteo.cuttle.{DatabaseConfig, Logger, Database => CoreDB}
  import doobie.implicits._

  type QueueData = (FlowCreator, Seq[ExecutionPlatform], Option[RetryStrategy], Option[Duration])

  /**
     Create a new scheduler manager
    * @param maxWorkflow max nb of workflow waiting to be run
    * @param dbConfig Environment of database
    * @param signalManager Used to send signal to a specific graph using the key of kafka as the workflow id
    * @return
    */
  def apply(maxWorkflow : Int, dbConfig : DatabaseConfig = DatabaseConfig.fromEnv)
           (signalManager : SignalManager[String, _])
           (implicit F : Concurrent[IO], logger : Logger) = {
    logger.info("Applying migrations to database")
    for {
      xa <- Stream.eval(CoreDB.connect(dbConfig)(logger))
      _  <- Stream.eval(FlowDB.doSchemaUpdates.transact(xa))
      _ = logger.info("Database up-to-date")
      sem <- Stream.eval(Semaphore[IO](1))
      queue <- Stream.eval(Queue.bounded[IO, QueueData](maxWorkflow))
    } yield new WorkflowsManager(queue, xa, signalManager)(sem)
  }
}