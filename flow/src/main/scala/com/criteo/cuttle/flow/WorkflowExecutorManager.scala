package com.criteo.cuttle.flow

import java.util.concurrent.ConcurrentHashMap

import com.criteo.cuttle.Executor

object WorkflowExecutorManager {

  private val workflowToJob = new ConcurrentHashMap[String, Executor[FlowScheduling]]

  def addWorkflowReference(workflowId: String, executor: Executor[FlowScheduling]) =
    workflowToJob.putIfAbsent(workflowId, executor)

  def getExecutorFromWf(workflowId: String): Executor[FlowScheduling] = workflowToJob.get(workflowId)
}