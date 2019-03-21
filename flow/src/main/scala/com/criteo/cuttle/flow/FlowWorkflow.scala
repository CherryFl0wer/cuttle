package com.criteo.cuttle.flow

import java.util.UUID

import com.criteo.cuttle.{Scheduling, Workload}
import com.criteo.cuttle.flow.FlowSchedulerUtils._
import io.circe._

/**
  * A timeseries workflow
  **/
trait FlowWorkflow extends Workload[FlowScheduling] {

  def all = vertices

  def uid = UUID.randomUUID()

  override def asJson(implicit se : Encoder[FlowJob]) = flowWFEncoder(se)(this)

  private[criteo] def vertices: Set[FlowJob]
  private[criteo] def edges: Set[Dependency]

  private[cuttle] def roots: Set[FlowJob] = {
    val childNodes = edges.map { case (child, _, _) => child }
    vertices.filter(!childNodes.contains(_))
  }

  private[cuttle] def leaves: Set[FlowJob] = {
    val parentNodes = edges.map { case (_, parent, _) => parent }
    vertices.filter(!parentNodes.contains(_))
  }

  // Returns a list of jobs in the workflow sorted topologically, using Kahn's algorithm. At the
  // same time checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[FlowJob] = graph.topologicalSort[FlowJob](
    vertices,
    edges.map { case (child, parent, _) => parent -> child }
  ) match {
    case Some(sortedNodes) => sortedNodes
    case None              => throw new IllegalArgumentException("Workflow has at least one cycle")
  }

  /**
    * Compose a [[FlowWorkflow]] with another [[FlowWorkflow]] but without any
    * dependency. It won't add any edge to the graph.
    *
    * @param otherWorflow The workflow to compose this workflow with.
    */
  def and(otherWorflow: FlowWorkflow): FlowWorkflow = {
    val leftWorkflow = this
    new FlowWorkflow {
      val vertices = leftWorkflow.vertices ++ otherWorflow.vertices
      val edges = leftWorkflow.edges ++ otherWorflow.edges
    }
  }

  /**
    * Compose a [[FlowWorkflow]] with a second [[FlowWorkflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * default dependency descriptors implicitly provided by the [[com.criteo.cuttle.Scheduling]] used by this workflow.
    *
    * @param rightWorkflow        The workflow to compose this workflow with.
    * @param dependencyDescriptor If injected implicitly, default dependency descriptor for the current [[com.criteo.cuttle.Scheduling]].
    */
  def dependsOn(rightWorkflow: FlowWorkflow)(implicit dependencyDescriptor: PriorityFactor): FlowWorkflow =
    dependsOn((rightWorkflow, dependencyDescriptor))

  /**
    * Compose a [[FlowWorkflow]] with a second [[FlowWorkflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * specified dependency descriptors.
    *
    * @param rightOperand The workflow to compose this workflow with.
    */
  def dependsOn(rightOperand: (FlowWorkflow, PriorityFactor)): FlowWorkflow = {
    val (rightWorkflow, depDescriptor) = rightOperand
    val leftWorkflow = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftWorkflow.roots
      v2 <- rightWorkflow.leaves
    } yield (v1, v2, depDescriptor)
    new FlowWorkflow {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges ++ newEdges
    }
  }
}

/** Utilities for [[FlowWorkflow]]. */
object FlowWorkflow {


  /** An empty [[Workflow]] (empty graph). */
  def empty[S <: Scheduling]: FlowWorkflow = new FlowWorkflow {
    def vertices = Set.empty
    def edges = Set.empty
  }

  /**
    * Validation of:
    * - absence of cycles in the workflow
    * - absence of jobs with the same id
    *  @return the list of errors in the workflow, if any
    */
  def validate(workflow: FlowWorkflow): List[String] = {
    val errors = collection.mutable.ListBuffer.empty[String]

    if (graph
      .topologicalSort[FlowJob](
      workflow.vertices,
      workflow.edges.map { case (child, parent, _) => parent -> child }
    )
      .isEmpty) {
      errors += "FlowWorkflow has at least one cycle"
    }

    graph
      .findStronglyConnectedComponents[FlowJob](
      workflow.vertices,
      workflow.edges.map { case (child, parent, _) => parent -> child }
    )
      .filter(scc => scc.size >= 2) // Strongly connected components with more than 2 jobs are cycles
      .foreach(scc => errors += s"{${scc.map(job => job.id).mkString(",")}} form a cycle")

    workflow.vertices.groupBy(_.id).collect {
      case (id: String, jobs) if jobs.size > 1 => id
    } foreach (id => errors += s"Id $id is used by more than 1 job")

    errors.toList
  }
}
