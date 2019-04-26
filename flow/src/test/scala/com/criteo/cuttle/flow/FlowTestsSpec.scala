package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.{Completed, Job, TestScheduling}
import scala.concurrent.duration._
import org.scalatest._
import com.criteo.cuttle.Utils.logger

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

  def waitingJob(time : FiniteDuration) : Job[FlowScheduling] = Job("job-waiting", FlowScheduling()) { implicit e =>
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


  test("it should execute in order the jobs correctly with waiting job") {
    val wf = (job(0) && waitingJob(3 seconds)) --> job(1) --> job(2)
    val project = FlowProject("test01", "Test of jobs execution")(wf)
    val browse = project.start[IO]().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0", "job-waiting"),
      List("job-waiting"),
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

  test("it should execute sequential `and`") {
    val wf = job(0) --> (job(1) && job(2)) --> (job(3) && job(4))
    val project = FlowProject("test02", "Test of jobs execution")(wf)
    val browse = project.start[IO]().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0"),
      List("job-1", "job-2"),
      List("job-2"),
      List("job-3", "job-4"),
      List("job-4"),
      List.empty
    )

    var x = 0
    browse.foreach { runnedJobs =>
      runnedJobs.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
      x += 1
    }
  }





}
