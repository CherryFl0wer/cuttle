package com.criteo.cuttle.flow

import com.criteo.cuttle.{Scheduling, Workload}
import com.criteo.cuttle.flow.FlowSchedulerUtils._
import io.circe._

/**
  * A DAG workflow
  **/

object RoutingKind {
  sealed trait Routing
  case object Success extends Routing
  case object Failure extends Routing

  val routes = Seq(Success, Failure)
}

trait FlowWorkflow extends Workload[FlowScheduling] {


  type Dependency = (FlowJob, FlowJob, RoutingKind.Routing)

  def all = vertices

  override def asJson(implicit se : Encoder[FlowJob]) = flowWFEncoder(se)(this)

  private[criteo] def vertices: Set[FlowJob]
  private[criteo] def edges: Set[Dependency]

  private[cuttle] def roots: Set[FlowJob] = {
    val nodes = edges.map { case (_, child, _) => child }
    vertices.filter(!nodes.contains(_))
  }

  private[cuttle] def leaves: Set[FlowJob] = {
    val nodes = edges.map { case (parent, _, _) => parent }
    vertices.filter(!nodes.contains(_))
  }


  // Returns a list of jobs in the workflow sorted topologically, using Kahn's algorithm. At the
  // same time checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[FlowJob] = {
    val edgeSuccess = edges.filter { case (_, _, kind) => kind == RoutingKind.Success }
    val verticesError = edges.filter { case (_, _, kind) => kind == RoutingKind.Failure }.map(_._2)

    val verticesSuccess = vertices -- verticesError

    graph.topologicalSort[FlowJob](
      verticesSuccess, edgeSuccess.map { case (parent, child, _) => parent -> child }
    ) match {
      case Some(sortedNodes) => sortedNodes
      case None => throw new IllegalArgumentException("Workflow has at least one cycle")
    }
  }


  private[cuttle] def childOf(vertice : FlowJob) : Set[FlowJob] = {
    if (leaves.contains(vertice)) Set.empty
    else edges.filter { case (current, _, _) => current == vertice }.map { case (_, child, _) => child }
  }

  private[cuttle] def parentsOf(vertice : FlowJob) : Set[FlowJob] = {
    if (roots.contains(vertice)) Set.empty
    else edges.filter { case (_, current, _) => current == vertice }.map { case (parent, _, _) => parent }
  }



  /**
    * Compose a [[FlowWorkflow]] with another [[FlowWorkflow]] but without any
    * dependency. It won't add any edge to the graph.
    *
    * @param otherWorflow The workflow to compose this workflow with.
    */

  def &&(other : FlowWorkflow) : FlowWorkflow = and(other)

  def and(otherWorflow: FlowWorkflow): FlowWorkflow = {
    val leftWorkflow = this
    new FlowWorkflow {
      val vertices = leftWorkflow.vertices ++ otherWorflow.vertices
      val edges = leftWorkflow.edges ++ otherWorflow.edges
    }
  }



  /**
    * Compose a [[FlowWorkflow]] with a second [[FlowWorkflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves.
    *
    * @param success The workflow to compose this workflow with.
    */

  def -->(success : FlowWorkflow) : FlowWorkflow = andThen((success, RoutingKind.Success))


  /**
    * Compose a [[FlowWorkflow]] with two [[FlowWorkflow]] one way lead to success the other to fail in case
    * of a job failure the scheduler will be able to redirect to the good path
    *
    * @param success If left [[FlowWorkflow]] is a succesful future then execute this one
    * @param fail In the other case execute this [[FlowWorkflow]]
    */
  def successAndError(success : FlowWorkflow, fail : FlowWorkflow) = {
    val leftWorkflow = this

    // We take the branch that goes to success to build from this specific(s) vertice(s)
    val successKindVertice = leftWorkflow.edges.filter { case (_, current, kind) => leftWorkflow.leaves.contains(current) && kind == RoutingKind.Success }.map(_._2)

    val newEdgesSucc: Set[Dependency] = for {
      v1 <- if (successKindVertice.isEmpty) leftWorkflow.leaves else successKindVertice
      v2 <- success.roots
    } yield (v1, v2, RoutingKind.Success)

    val newEdgesFail: Set[Dependency] = for {
      v1 <- if (successKindVertice.isEmpty) leftWorkflow.leaves else successKindVertice
      v2 <- fail.roots
    } yield (v1, v2, RoutingKind.Failure)

    new FlowWorkflow {
      val vertices = leftWorkflow.vertices ++ success.vertices ++ fail.vertices
      val edges = leftWorkflow.edges ++ success.edges ++ fail.edges ++ newEdgesSucc ++ newEdgesFail
    }
  }

  def andThen(rightOperand : (FlowWorkflow, RoutingKind.Routing)): FlowWorkflow = {

    val leftWorkflow = this
    val (rightWorkflow, kindRoute) = rightOperand

    val onlyKindVertice = leftWorkflow.edges.filter { case (_, current, kind) => leftWorkflow.leaves.contains(current) && kind == kindRoute }.map(_._2)
    val newEdges: Set[Dependency] = for {
      v1 <- if (onlyKindVertice.isEmpty) leftWorkflow.leaves else onlyKindVertice
      v2 <- rightWorkflow.roots
    } yield (v1, v2, kindRoute)

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
    if (jobs.isEmpty)
      wf

    val newVertices = wf.vertices.filterNot(jobs)
    val newEdges = wf.edges.filterNot(p => jobs.contains(p._1))

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
      workflow.edges.map { case (parent, child, _) => child -> parent }
    )
      .isEmpty) {
      errors += "FlowWorkflow has at least one cycle"
    }

    graph
      .findStronglyConnectedComponents[FlowJob](
      workflow.vertices,
      workflow.edges.map { case (parent, child, _) => child -> parent }
    )
      .filter(scc => scc.size >= 2) // Strongly connected components with more than 2 jobs are cycles
      .foreach(scc => errors += s"{${scc.map(job => job.id).mkString(",")}} form a cycle")

    workflow.vertices.groupBy(_.id).collect {
      case (id: String, jobs) if jobs.size > 1 => id
    } foreach (id => errors += s"Id $id is used by more than 1 job")

    errors.toList
  }
}
