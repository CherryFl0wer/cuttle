package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.{Execution, Finished, ITTestScheduling, Job, Output}

import scala.concurrent.duration._
import org.scalatest._
import com.criteo.cuttle.Utils.logger
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

/**
  * @summary Functionals tests of the sub-library of scheduling `Flow` placed in [[com.criteo.cuttle.flow._]]
  *          - Testing workflow "browsing"
  *          - Testing job execution
  *
  *           PS: Job's are not sophisticated, they are simple,
  *           they are here to assure that execution is conform at what we expect
  *
  * @Todo specific database for the tests
  * */
class FlowTestsSpec extends FunSuite with ITTestScheduling with Matchers {


  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"job-${i.toString}", FlowScheduling())(completed))


  val failedSideEffect: (Execution[_]) => Future[Nothing] = (_ : Execution[_]) => Future.failed(new Exception("Failed task"))
  val failedJob: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"failed-${i.toString}", FlowScheduling())(failedSideEffect))

  def waitingJob(id: String = "0", time : FiniteDuration) : Job[FlowScheduling] = Job(s"job-waiting-${id}", FlowScheduling()) { implicit e =>
    e.park(time).map(_ => Finished)(ExecutionContext.global)
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

  test("it should validate workflow and operand with error edge") {
    val workflow = job(0) --> job(1).error(job(3)) --> job(2)

    workflow.jobsInOrder.map(_.id) should contain theSameElementsInOrderAs List("job-0", "job-1", "job-2")

    workflow.childsOf(job(1)).map(_.id) should contain theSameElementsAs List("job-2", "job-3-from-job-1")

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate workflow and operand with same error job") {
    val workflow = job(0).error(job(3)) --> job(1) --> job(2).error(job(3))

    workflow.jobsInOrder.map(_.id) should contain theSameElementsInOrderAs List("job-0", "job-1", "job-2")

    workflow.childsOf(job(0)).map(_.id) should contain theSameElementsAs List("job-1", "job-3-from-job-0")

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate a more complex workflow") {
    val errorJob1 = job(9)
    val errorJob2 = job(8)

    val wf = job(0).error(errorJob1) && job(1).error(errorJob1)
    val wf2 = (wf --> job(2).error(errorJob1)) && job(3).error(errorJob2)
    val workflow = wf2 --> job(4).error(errorJob2)

    workflow.jobsInOrder.map(_.id) should contain theSameElementsInOrderAs List("job-3", "job-0", "job-1", "job-2", "job-4")

    val err = workflow.childsOf(job(1)).filter(j => j.id.startsWith(errorJob1.id)).head // .error(FlowJob) change the name of the job

    workflow.parentsOf(err).map(_.id) should contain theSameElementsAs List("job-1")

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate a more complex workflow with an error binded on two job") {
    val errorJob1 = job(9)
    val errorJob2 = job(8)

    val wf = (job(0) && job(1)).error(errorJob1)
    val wf2 = (wf --> job(2).error(errorJob1)) && job(3).error(errorJob2)
    val workflow = wf2 --> job(4).error(errorJob2)

    workflow.jobsInOrder.map(_.id) should contain theSameElementsInOrderAs List("job-3", "job-0", "job-1", "job-2", "job-4")

    val err = workflow.childsOf(job(1)).filter(j => j.id.startsWith(errorJob1.id)).head // .error(FlowJob) change the name of the job

    workflow.parentsOf(err).map(_.id) should contain theSameElementsAs List("job-1", "job-0")

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
    val wf = (job(0) && waitingJob("0", 3.seconds)) --> job(1) --> job(2)
    val project = FlowProject("test01", "Test of jobs execution")(wf)
    val browse = project.start().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0", "job-waiting-0"),
      List("job-waiting-0"),
      List("job-1"),
      List("job-2")
    )

    var x = 0

    browse.slice(0, browse.length-1).foreach { runnedJobs =>
      runnedJobs should be ('right)
      val jobs = runnedJobs.right.get
      jobs.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
      x += 1
    }

    browse.last shouldBe Right(Set.empty)
  }


  test("it should execute sequential `and` (success only)") {
    val wf = job(0) --> (job(1) && job(2)) --> (job(3) && job(4))
    val project = FlowProject("test02", "Test  of jobs execution")(wf)
    val browse = project.start().compile.toList.unsafeRunSync
    val testingList = List(
      List("job-0"),

      List("job-1", "job-2"),
      List("job-1", "job-2"),

      List("job-3", "job-4"),
      List("job-3", "job-4")
    )


    var x = 0

    browse.slice(0, browse.length-1).foreach { runnedJobs =>
      runnedJobs should be ('right)
      val jobs = runnedJobs.right.get
      jobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
      x += 1
    }

    browse.last shouldBe Right(Set.empty)
  }

  test("it should execute in order with multiple waiting job (success only) ") {
    val wf = (job(0) && job(1) && waitingJob("0",3.seconds)) --> job(2) --> (job(3) && waitingJob("1", 5.seconds)) --> job(4) --> job(5)
    val project = FlowProject("test03", "Test  of jobs execution")(wf)
    val browse = project.start().compile.toList.unsafeRunSync
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
      runnedJobs should be ('right)
      val jobs = runnedJobs.right.get
      jobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
      x += 1
    }
    browse.last shouldBe Right(Set.empty)
  }


  test("it should execute a tree like form (success only)") {

    val part1 = (job(0) && waitingJob(time = 3.seconds)) --> job(3)
    val part2 = job(2) --> job(4)
    val wf = (part1 && part2) --> job(5)

    val project = FlowProject("test02", "Test  of jobs execution")(wf)
    val browse = project.start().compile.toList.unsafeRunSync

    val testingList = List(
      List("job-0", "job-2", "job-waiting-0"),
      List("job-2", "job-waiting-0"),
      List("job-waiting-0", "job-4"),
      List("job-waiting-0"),
      List("job-3"),
      List("job-5")
    )

    var x = 0

    browse.slice(0, browse.length-1).foreach { runnedJobs =>
      runnedJobs should be ('right)
      val jobs = runnedJobs.right.get
      jobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
      x += 1
    }

    browse.last shouldBe Right(Set.empty)
  }


  test("it should ") {
    import io.circe.syntax._

    val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs)  + " passed to step two"
      Future.successful(Output(Json.obj("aud" -> x.asJson)))
    }

    val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
      Future.successful(Output(Json.obj("aud" -> "albuquerque".asJson)))
    }

    val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("pedo" -> "velo".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      val y = e.optic.pedo.string.getOption(in)
      val x = e.optic.aud.string.getOption(in)  + " passed to step three"
      Future.successful(Output(x.asJson))
    }

    val wf = (job1 && job1bis) --> job2

    val project = FlowProject("test03", "Test of jobs I/O")(wf)
    val browse = project.start().compile.toList.unsafeRunSync

  }

  //TODO Add tests on

  //TODO Add a test fail on a success only workflow
}
