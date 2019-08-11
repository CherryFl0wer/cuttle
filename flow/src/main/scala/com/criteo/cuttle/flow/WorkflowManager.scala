package com.criteo.cuttle.flow

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO}
import com.criteo.cuttle.flow.signals.SignalManager
import com.criteo.cuttle.flow.{Database => FlowDB}
import com.criteo.cuttle.XA
import com.criteo.cuttle.flow.FlowSchedulerUtils.JobState
import fs2.Stream
import fs2.concurrent.Queue


class WorkflowManager(workflowToRun : Queue[IO, FlowCreator],
                      semaphore: Semaphore[IO],
                      transactorQuery : XA,
                      signalManager : SignalManager[String, _])(implicit F : Concurrent[IO]) {


  /**
    * Add a graph to the workflow manager
    * @param graph The graph
    * @return
    */
  def push(graph : FlowCreator): IO[Unit] = workflowToRun.enqueue1(graph)

  /**
    * Dequeue the Workflow in the internal queue and run them in parallel.
    * @param parallelRun Number of workflow runned at the same time
    * @param C
    * @return
    */
  def parRun(parallelRun : Int): Stream[IO, Either[Throwable, (FlowWorkflow, JobState)]] =
    workflowToRun
      .dequeue
      .mapAsync(parallelRun) {
        graph =>
          for {
            _ <- signalManager.newTopic(graph.workflowId)
            executionLog <- graph.parStart(semaphore)
            _ <- signalManager.removeTopic(graph.workflowId)
          } yield executionLog
      }

  /**
    * Run a single workflow
    * @param graph a Workflow ready to be run
    * @return
    */
  def runOne(graph : FlowCreator): IO[Either[Throwable, (FlowWorkflow, JobState)]] =
    for {
      _ <- IO(println(s"Adding ${graph.workflowId}"))
      _ <- signalManager.newTopic(graph.workflowId)
      executionLog <- graph.parStart(semaphore)
      _ <- signalManager.removeTopic(graph.workflowId)
    } yield executionLog


  /*
  def runSingleJob(graph : FlowCreator, jobId: String, input : Option[Json] = None) =  for {
    _ <- signalManager.newTopic(graph.workflowId)
    executionLog <- graph.parStart(semaphore).compile.toList
    _ <- signalManager.removeTopic(graph.workflowId)
  } yield executionLog*/
}

object WorkflowManager {

  import com.criteo.cuttle.Logger

  /**
     Create a new scheduler manager
    * @param maxWorkflow max nb of workflow waiting to be run
    * @param xa XA
    * @param signalManager Used to send signal to a specific group of workflow using the key of kafka as the workflow id
    * @return
    */
  def apply(xa : XA, signalManager : SignalManager[String, _])(maxWorkflow : Int = 20)
           (implicit F : Concurrent[IO], logger : Logger): IO[WorkflowManager] = {
    import doobie.implicits._
    for {
      _     <- FlowDB.doSchemaUpdates.transact(xa)
      sem   <- Semaphore[IO](1)
      queue <- Queue.bounded[IO, FlowCreator](maxWorkflow)
    } yield new WorkflowManager(queue, sem, xa, signalManager)
  }
}