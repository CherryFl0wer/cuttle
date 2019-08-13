package com.criteo.cuttle.flow

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO}
import com.criteo.cuttle.flow.signals.SignalManager
import com.criteo.cuttle.flow.{Database => FlowDB}
import com.criteo.cuttle.{Logger, XA}
import com.criteo.cuttle.flow.FlowSchedulerUtils.JobState

class FlowManager(semaphore: Semaphore[IO],
                  signalManager : SignalManager[String, _])(implicit F : Concurrent[IO], logger : Logger) {

  /**
    * Run a single job
    * @param jobId
    * @param graph
    * @return
    */
  def runJobFromFlow(jobId : String, graph : FlowExecutor): IO[Either[Throwable, (FlowWorkflow, JobState)]] = {
    for {
      _ <- signalManager.newTopic(graph.workflowId)
      executionLog <- graph.runSingleJob(jobId)
      _ <- signalManager.removeTopic(graph.workflowId)
    } yield executionLog
  }


  /**
    * Run a single workflow
    *
    * @param graph a Workflow ready to be run
    * @return
    */
  def runOne(graph : FlowExecutor): IO[Either[Throwable, (FlowWorkflow, JobState)]] =
    for {
      _ <- signalManager.newTopic(graph.workflowId)
      executionLog <- graph.parStart(semaphore)
      _ <- signalManager.removeTopic(graph.workflowId)
    } yield executionLog

}

object FlowManager {

  import com.criteo.cuttle.Logger

  /**
    * Create a new scheduler manager
    * SchedulerManager is used to run workflow that need to use signal
    * by creating a topic in the signal manager, executing workflow and deleting the workflow' topic
    * @param xa XA
    * @param signalManager Used to send signal to a specific group of workflow using the key of kafka as the workflow id
    * @return
    */
  def apply(xa : XA, signalManager : SignalManager[String, _])
           (implicit F : Concurrent[IO], logger : Logger): IO[FlowManager] = {
    import doobie.implicits._
    for {
      _     <- FlowDB.doSchemaUpdates.transact(xa)
      sem   <- Semaphore[IO](1)
    } yield new FlowManager(sem, signalManager)
  }
}