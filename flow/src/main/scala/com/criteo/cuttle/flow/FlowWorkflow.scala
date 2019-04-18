package com.criteo.cuttle.flow

import com.criteo.cuttle.{Scheduling, Workload}
import com.criteo.cuttle.flow.FlowSchedulerUtils._
import io.circe._

/**
  * A timeseries workflow
  **/
trait FlowWorkflow extends Workload[FlowScheduling] {


  type Dependency = (FlowJob, FlowJob)

  def all = vertices

  override def asJson(implicit se : Encoder[FlowJob]) = flowWFEncoder(se)(this)

  private[criteo] def vertices: Set[FlowJob]
  private[criteo] def edges: Set[Dependency]

  private[cuttle] def roots: Set[FlowJob] = {
    val parentNodes = edges.map { case (parent, _) => parent }
    vertices.filter(!parentNodes.contains(_))
  }

  private[cuttle] def leaves: Set[FlowJob] = {
    val childNodes = edges.map { case (_, child) => child }
    vertices.filter(!childNodes.contains(_))
  }


  // Returns a list of jobs in the workflow sorted topologically, using Kahn's algorithm. At the
  // same time checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[FlowJob] = graph.topologicalSort[FlowJob](
    vertices,
    edges.map { case (parent, child) => child -> parent }
  ) match {
    case Some(sortedNodes) => sortedNodes
    case None              => throw new IllegalArgumentException("Workflow has at least one cycle")
  }


  private[cuttle] def childOf(vertice : FlowJob) : Set[FlowJob] = {
    if (roots.contains(vertice)) Set.empty
    else edges.filter { case (parent, _) => parent == vertice }.map { case (_, child) => child }
  }



  /**
    * Compose a [[FlowWorkflow]] with another [[FlowWorkflow]] but without any
    * dependency. It won't add any edge to the graph.
    *
    * @param otherWorflow The workflow to compose this workflow with.
    */

  def ::(other : FlowWorkflow) : FlowWorkflow = and(other)

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
  /* DEAD CODE
  def dependsOn(rightWorkflow: FlowWorkflow)(implicit dependencyDescriptor: Json): FlowWorkflow =
    dependsOn((rightWorkflow, dependencyDescriptor))
  */


  /**
    * Compose a [[FlowWorkflow]] with a second [[FlowWorkflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * specified dependency descriptors.
    *
    * @param rightOperand The workflow to compose this workflow with.
    */

  def <--(right : FlowWorkflow) : FlowWorkflow = dependsOn(right)

  def dependsOn(rightWorkflow: FlowWorkflow): FlowWorkflow = {
    val leftWorkflow = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftWorkflow.roots
      v2 <- rightWorkflow.leaves
    } yield (v1, v2)
    new FlowWorkflow {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges ++ newEdges
    }
  }



}

/** Utilities for [[FlowWorkflow]]. */
object FlowWorkflow {


  /** An empty [[FlowWorkflow]] (empty graph). */
  def empty[S <: Scheduling]: FlowWorkflow = new FlowWorkflow {
    def vertices = Set.empty
    def edges = Set.empty
  }


  def without(wf : FlowWorkflow, jobs : Set[FlowJob]) : FlowWorkflow = {
    if (jobs.isEmpty) wf

    val newVertices = wf.vertices.filterNot(jobs)
    val newEdges = wf.edges.filterNot(p => jobs.contains(p._2))

    new FlowWorkflow {
      def vertices = newVertices
      def edges = newEdges
    }
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
      workflow.edges.map { case (parent, child) => child -> parent }
    )
      .isEmpty) {
      errors += "FlowWorkflow has at least one cycle"
    }

    graph
      .findStronglyConnectedComponents[FlowJob](
      workflow.vertices,
      workflow.edges.map { case (parent, child) => child -> parent }
    )
      .filter(scc => scc.size >= 2) // Strongly connected components with more than 2 jobs are cycles
      .foreach(scc => errors += s"{${scc.map(job => job.id).mkString(",")}} form a cycle")

    workflow.vertices.groupBy(_.id).collect {
      case (id: String, jobs) if jobs.size > 1 => id
    } foreach (id => errors += s"Id $id is used by more than 1 job")

    errors.toList
  }
}
