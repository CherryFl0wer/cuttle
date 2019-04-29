package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.{Completed, Job, TestScheduling}

import scala.concurrent.duration._
import org.scalatest._
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow.FlowSchedulerUtils.RunJob

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * @summary Functionals tests of the sub-library of scheduling `Flow` situated in [com.criteo.cuttle.flow._]
  *          - Testing workflow "browsing"
  *          - Testing job execution
  *
  *           PS: Job's are not sophisticated, they are simple,
  *           they are here to assure that execution is conform at what we expect
  *
  * @Todo Own database for the tests
  * */
class FlowTestsSpec extends FunSuite with TestScheduling with Matchers {


  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(6)(i => Job(s"job-${i.toString}", FlowScheduling())(completed))

  def waitingJob(id: String = "0", time : FiniteDuration) : Job[FlowScheduling] = Job(s"job-waiting-${id}", FlowScheduling()) { implicit e =>
    e.park(time).map(_ => Completed)(ExecutionContext.global)
  }


  test("it should validate empty workflow") {
    val workflow = FlowWorkflow.empty

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate unit workflow") {
    val workflow = job(0)

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }


  test("it should validate workflow without cycles") {
    val workflow = job(0) --> job(1) --> job(2)

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate workflow order") {
    val workflow = job(0) --> job(1) --> job(2)

    val order = workflow.jobsInOrder

    order.map(_.id) should contain theSameElementsInOrderAs List("job-0", "job-1", "job-2")

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate workflow and operand with error branch") {
    val workflow = job(0) --> job(1) successAndError (job(2), job(3))

    workflow.jobsInOrder.map(_.id) should contain theSameElementsInOrderAs List("job-0", "job-1", "job-2")

    workflow.childOf(job(1)).map(_.id) should contain theSameElementsAs List("job-2", "job-3")

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }



  test("it should validate workflow without cycles (one parent with many children)") {
    val job1: Vector[Job[FlowScheduling]] =
      Vector.tabulate(10)(i => Job(s"job-${i}", FlowScheduling())(completed))
    val workflow = (0 to 8).map(i => job1(9) --> job1(i)).reduce(_ && _)

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it shouldn't validate cyclic workflow") {
    val workflow = job(0) --> job(1) --> job(2) --> job(0)

    assert(FlowSchedulerUtils.validate(workflow).isLeft, "workflow passed a validation of cycle presence")
  }


  test("it should execute in order the jobs correctly with waiting job (success only)") {
    val wf = (job(0) && waitingJob("0", 3 seconds)) --> job(1) --> job(2)
    val project = FlowProject("test01", "Test of jobs execution")(wf)
    val browse = project.start[IO]().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0", "job-waiting-0"),
      List("job-waiting-0"),
      List("job-1"),
      List("job-2"),
      List.empty
    )

    var x = 0
    browse.foreach { runnedJobs =>
        runnedJobs.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
        x += 1
    }
  }


  test("it should execute sequential `and` (success only)") {
    val wf = job(0) --> (job(1) && job(2)) --> (job(3) && job(4))
    val project = FlowProject("test02", "Test  of jobs execution")(wf)
    val browse = project.start[IO]().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0"),

      List("job-1", "job-2"),
      List("job-1", "job-2"),

      List("job-3", "job-4"),
      List("job-3", "job-4")
    )

    var x = 0
    browse.slice(0, browse.length-1).foreach { runnedJobs =>
      runnedJobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
      x += 1
    }

    browse.last should contain theSameElementsAs List.empty
  }

  test("it should execute in order with multiple waiting job (success only) ") {
    val wf = (job(0) && job(1) && waitingJob("0",3 seconds)) --> job(2) --> (job(3) && waitingJob("1", 5 seconds)) --> job(4) --> job(5)
    val project = FlowProject("test03", "Test  of jobs execution")(wf)
    val browse = project.start[IO]().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0", "job-1", "job-waiting-0"),
      List("job-0", "job-1", "job-waiting-0"),
      List("job-0", "job-1", "job-waiting-0"),
      List("job-2"),
      List("job-3", "job-waiting-1"),
      List("job-3", "job-waiting-1"),
      List("job-4"),
      List("job-5")
    )

    var x = 0
    browse.slice(0, browse.length-1).foreach { runnedJobs =>
      runnedJobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
      x += 1
    }

    browse.last should contain theSameElementsAs List.empty
  }

  /*
  test("it should execute a tree like form (success only)") {
    val part1 = (job(0) && waitingJob("1", 3 seconds)) --> job(3)
    val part2 = job(2) --> job(4)
    val wf = (part1 && part2) --> job(5)

    val project = FlowProject("test02", "Test  of jobs execution")(wf)
    val browse = project.start[IO]().compile.toList.unsafeRunSync
  } */

}
