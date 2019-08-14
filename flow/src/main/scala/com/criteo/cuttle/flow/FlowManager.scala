package com.criteo.cuttle.flow

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO}
import com.criteo.cuttle.flow.signals.SignalManager
import com.criteo.cuttle.flow.{Database => FlowDB}

import com.criteo.cuttle.{Logger, XA}
import com.criteo.cuttle.flow.FlowSchedulerUtils.JobState

class FlowManager(semaphore: Semaphore[IO], xa : XA,
                  signalManager : SignalManager[String, _])(implicit F : Concurrent[IO], logger : Logger) {

  /**
    * Run a single job
    * @param jobId
    * @param graph
    * @return
    */
  def runJobFromFlow(jobId : String, graph : FlowExecutor): IO[Either[Throwable, (FlowWorkflow, JobState)]] = {
    val job = graph.jobs.vertices.find(j => j.id == jobId)
    if (job.isEmpty) IO.pure(Left(new Throwable(s"$jobId does not exist")))

    for {
      _ <- signalManager.newTopic(graph.workflowId)
      executionLog <- graph.runSingleJob(jobId)
      _ <- signalManager.removeTopic(graph.workflowId)
    } yield executionLog
  }


  /**
    * @deprecated
    *
    * @param jobId
    * @param workflowId
    * @param graph
    * @param initialInput
    * @return
    */
  def runJobFromWfId(jobId : String,
               workflowId : String,
               graph : FlowWorkflow,
               initialInput : Option[io.circe.Json] = None): IO[Either[Throwable, (FlowWorkflow, JobState)]] = {
    import io.circe.Json
    import doobie.imports._

    val job = graph.vertices.find(j => j.id == jobId)
    if (job.isEmpty) IO.pure(Left(new Throwable(s"$jobId does not exist")))
    else for {
      // Get outputs from parents jobs and put them as input
      outputsInWorkflow <- FlowDB.retrieveWorkflowResults(workflowId).transact(xa)
      parentOfJob = graph.parentsOf(job.get).map(_.id)
      outputsFromJobs = outputsInWorkflow.filter(j => parentOfJob.contains(j._1))

      inputInit = initialInput.fold(Json.Null)(identity)
      jsonInput = FlowSchedulerUtils.mergeDuplicateJson((jobId, inputInit), outputsFromJobs)
      newJobWithInput = job.get.copy(scheduling = FlowScheduling(inputs = jsonInput._2))(job.get.effect)
      newGraph = graph replace newJobWithInput
      flowExecution <- FlowExecutor(xa)(newGraph)
      execution <- flowExecution.runSingleJob(jobId)
     } yield execution

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
    } yield new FlowManager(sem, xa, signalManager)
  }
}