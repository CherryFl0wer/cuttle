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

  val choices = Seq(Success, Failure)
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


  private[cuttle] def pathFromVertice(job : FlowJob, by: RoutingKind.Routing): Set[FlowJob]  = {
    val childs = childsFromRoute(job, by)
    if (!childs.isEmpty)
      childs.foldLeft(Set.empty[FlowJob]) { case (acc, job) => acc + job ++ pathFromVertice(job, by) }
    else
      Set.empty
  }

  private[cuttle] def childFrom(kind : RoutingKind.Routing) : Set[FlowJob] =
    edges
      .filter { case (_, _, route) => route == kind }
      .map { case (_, child, _) => child }

  private[cuttle] def childsFromRoute(job : FlowJob, kind: RoutingKind.Routing)  = edges
    .filter { case (current, _, route) => current == job && route == kind }
    .map { case (_, child, _) => child }

  private[cuttle] def childsOf(vertice : FlowJob) : Set[FlowJob] = {
    if (leaves.contains(vertice)) Set.empty
    else edges.filter { case (current, _, _) => current == vertice }.map { case (_, child, _) => child }
  }

  private[cuttle] def parentsOf(vertice : FlowJob) : Set[FlowJob] = {
    if (roots.contains(vertice)) Set.empty
    else {
      val x = edges.filter { case (_, current, _) => current == vertice }
      x.map { case (parent, _, _) => parent }
    }
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
  def andThen(rightOperand : (FlowWorkflow, RoutingKind.Routing)): FlowWorkflow = {

    val leftWorkflow = this
    val (rightWorkflow, kindRoute) = rightOperand

    val routingWorkflow = FlowWorkflow.withKind(leftWorkflow, kindRoute)

    val newEdges: Set[Dependency] = for {
      v1 <- routingWorkflow.leaves
      v2 <- rightWorkflow.roots
    } yield (v1, v2, kindRoute)

    new FlowWorkflow {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges ++ newEdges
    }
  }

  /**
    * Compose a [[FlowWorkflow]] and an job that will managed error coming from `this.leaves`
    * error will also change `fail`.id to avoid having the same name in the database causing backslash
    * @param fail the error job
    * */
  def error(fail : FlowWorkflow) = {
    val leftWorkflow = this

    if (fail.roots.isEmpty) leftWorkflow

    val job = fail.roots.head // supposed to have only one error
    val newJob = job.copy(id = job.id + s"-from-${leftWorkflow.leaves.map(_.id).mkString("-")}")(job.effect) // To avoid breaking at execution when error have the same id

    val newEdgesFail: Set[Dependency] = for {
      v1 <- leftWorkflow.leaves
    } yield (v1, newJob, RoutingKind.Failure)

    new FlowWorkflow {
      val vertices = leftWorkflow.vertices + newJob
      val edges = leftWorkflow.edges ++ newEdgesFail
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


  /** *
    * Take off every vertices in jobs and return the workflow without it
    *
    * @param wf
    * @param jobs
    * @return a new workflow without the `jobs`
    */
  def without(wf: FlowWorkflow, jobs: Set[FlowJob]): FlowWorkflow = {
    if (jobs.isEmpty)
      wf

    val newVertices = wf.vertices.filterNot(jobs)
    val newEdges = wf.edges.filterNot(p => jobs.contains(p._1))

    new FlowWorkflow {
      def vertices = newVertices

      def edges = newEdges
    }
  }


  /** *
    * Take off every jobs that does not come from a `kind` edge
    * @param wf
    * @param kind
    * @return a new workflow without the `jobs` from `kind`
    */
  def withKind(wf : FlowWorkflow, kind : RoutingKind.Routing) = {

    val setOfVertice = wf.edges.foldLeft(Set.empty[FlowJob]) { case (acc, edge) =>
      val (parent, child, route) = edge
      if (route == kind)
        acc + parent + child
      else
        acc + parent
    }

    val newVertices = if (setOfVertice.isEmpty) wf.roots else setOfVertice
    val newEdges = wf.edges.filter(p => p._3 == kind)

    new FlowWorkflow {
      def vertices = newVertices
      def edges = newEdges
    }
  }

  /**
    * Validation of:
    * - absence of cycles in the workflow
    * - absence of jobs with the same id
    *
    * @return the list of errors in the workflow, if any
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

    workflow.edges
      .map { case (_, child, kind) => (child, kind) }
      .groupBy(_._1)
      .foreach {
        case (job, values) => {
          val kindSet = values.map(_._2)
          val both = kindSet.exists { kind => kind == RoutingKind.Success } && kindSet.exists { kind => kind == RoutingKind.Failure }
          if (both) errors += s"${job.id} is both an error job and a success job"
        }
      }

    errors.toList
  }
}
