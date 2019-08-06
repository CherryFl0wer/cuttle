package com.criteo.cuttle.flow

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO}
import com.criteo.cuttle.flow.FlowSchedulerUtils.{FlowJob, RunJob}
import com.criteo.cuttle.flow.signals.SignalManager
import com.criteo.cuttle.flow.{Database => FlowDB}
import com.criteo.cuttle.{Completed, XA}
import fs2.Stream
import fs2.concurrent.{Queue, SignallingRef}

import scala.concurrent.Future

class WorkflowsManager(workflowToRun : Queue[IO, FlowCreator],
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
  def parRun(parallelRun : Int): Stream[IO, List[Either[Throwable, Set[RunJob]]]] =
    workflowToRun
      .dequeue
      .mapAsync(parallelRun) {
        graph =>
          for {
            _ <- signalManager.newTopic(graph.workflowId)
            executionLog <- graph.parStart(semaphore).compile.toList
            _ <- signalManager.removeTopic(graph.workflowId)
          } yield executionLog
      }

  /**
    * Run a single workflow
    * @param graph a Workflow ready to be run
    * @param C
    * @return
    */
  def runOne(graph : FlowCreator) =
    for {
      _ <- signalManager.newTopic(graph.workflowId)
      executionLog <- graph.parStart(semaphore).compile.toList
      _ <- signalManager.removeTopic(graph.workflowId)
    } yield executionLog


  /*
  def runSingleJob(graph : FlowCreator, jobId: String, input : Option[Json] = None) =  for {
    _ <- signalManager.newTopic(graph.workflowId)
    executionLog <- graph.parStart(semaphore).compile.toList
    _ <- signalManager.removeTopic(graph.workflowId)
  } yield executionLog*/
}

object WorkflowsManager {

  import com.criteo.cuttle.Logger

  /**
     Create a new scheduler manager
    * @param maxWorkflow max nb of workflow waiting to be run
    * @param xa XA
    * @param signalManager Used to send signal to a specific group of workflow using the key of kafka as the workflow id
    * @return
    */
  def apply(xa : XA, signalManager : SignalManager[String, _])(maxWorkflow : Int = 20)
           (implicit F : Concurrent[IO], logger : Logger): Stream[IO, WorkflowsManager] = {
    import doobie.implicits._
    for {
      _ <- Stream.eval(FlowDB.doSchemaUpdates.transact(xa))
      sem   <- Stream.eval(Semaphore[IO](1))
      queue <- Stream.eval(Queue.bounded[IO, FlowCreator](maxWorkflow))
    } yield new WorkflowsManager(queue, sem, xa, signalManager)
  }
}