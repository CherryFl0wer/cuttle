package com.criteo.cuttle.flow

import java.util.concurrent.ConcurrentHashMap

import com.criteo.cuttle.Executor

object WorkflowSchedulerManager {

  private val workflowToJob = new ConcurrentHashMap[String, FlowScheduler]

  def putWorkflow(workflowId: String, scheduler: FlowScheduler) =
    workflowToJob.putIfAbsent(workflowId, scheduler)

  def schedulerFrom(workflowId: String): FlowScheduler = workflowToJob.get(workflowId)
}