package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.{Execution, Finished, ITTestScheduling, Job, Output}

import scala.concurrent.duration._
import org.scalatest._
import com.criteo.cuttle.Utils.logger
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

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


  implicit val ctx = ExecutionContext.global
  implicit val timer: Timer[IO] = IO.timer(ctx)
  implicit val cs: ContextShift[IO] = IO.contextShift(ctx)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"job-${i.toString}", FlowScheduling())(completed))


  val failedSideEffect: Execution[_] => Future[Nothing] = (_ : Execution[_]) => Future.failed(new Exception("Failed task"))
  val failedJob: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"failed-${i.toString}", FlowScheduling())(failedSideEffect))

  def waitingJob(id: String = "0", time : FiniteDuration) : Job[FlowScheduling] = Job(s"job-waiting-${id}", FlowScheduling()) { implicit e =>
    e.park(time).map(_ => Finished)
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
    val testingList = List(
      List("job-0", "job-waiting-0"),
      List("job-waiting-0"),
      List("job-1"),
      List("job-2")
    )


    val program = for {
      project <- FlowGraph("test01", "Test of jobs execution")(wf)
      browse  <- project.start().compile.toList
    } yield {

      var x = 0
      browse.slice(0, browse.length-1).foreach { ranJobs =>
        ranJobs should be ('right)
        val jobs = ranJobs.right.get
        jobs.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
        x += 1
      }

      browse.last shouldBe Right(Set.empty)
    }

    program.unsafeRunSync()
  }


  test("it should execute sequential `and` (success only)") {
    val wf = job(0) --> (job(1) && job(2)) --> (job(3) && job(4))
    val testingList = List(
      List("job-0"),

      List("job-1", "job-2"),
      List("job-1", "job-2"),

      List("job-3", "job-4"),
      List("job-3", "job-4")
    )


    val program = for {
      project <- FlowGraph("test02", "Test of jobs execution")(wf)
      browse  <- project.start().compile.toList
    } yield {

      var x = 0
      browse.slice(0, browse.length-1).foreach { ranJobs =>
        ranJobs should be ('right)
        val jobs = ranJobs.right.get
        jobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
        x += 1
      }

      browse.last shouldBe Right(Set.empty)
    }

    program.unsafeRunSync()
  }

  test("it should execute in order with multiple waiting job (success only) ") {
    val wf = (job(0) && job(1) && waitingJob("0",3.seconds)) --> job(2) --> (job(3) && waitingJob("1", 5.seconds)) --> job(4) --> job(5)
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

    val program = for {
      project <- FlowGraph("test03", "Test of jobs execution")(wf)
      browse  <- project.start().compile.toList
    } yield {

      var x = 0
      browse.slice(0, browse.length-1).foreach { ranJobs =>
        ranJobs should be ('right)
        val jobs = ranJobs.right.get
        jobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
        x += 1
      }

      browse.last shouldBe Right(Set.empty)
    }

    program.unsafeRunSync()
  }


  test("it should execute a tree like form (success only)") {

    val part1 = (job(0) && waitingJob(time = 3.seconds)) --> job(3)
    val part2 = job(2) --> job(4)
    val wf = (part1 && part2) --> job(5)

    val testingList = List(
      List("job-0", "job-2", "job-waiting-0"),
      List("job-2", "job-waiting-0"),
      List("job-waiting-0", "job-4"),
      List("job-waiting-0"),
      List("job-3"),
      List("job-5")
    )

    val program = for {
      project <- FlowGraph("test04", "Test of jobs execution")(wf)
      browse  <- project.start().compile.toList
    } yield {

      var x = 0
      browse.slice(0, browse.length-1).foreach { ranJobs =>
        ranJobs should be ('right)
        val jobs = ranJobs.right.get
        jobs.toList.map(_._1.id) should contain atLeastOneElementOf testingList(x)
        x += 1
      }

      browse.last shouldBe Right(Set.empty)
    }

    program.unsafeRunSync()
  }


  test("it should check if output are fine at the end") {
    import io.circe.syntax._

    val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get  + " will pass to step two"
      Future.successful(Output(Json.obj("audience" -> x.asJson)))
    }

    val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
      Future.successful(Output(Json.obj("audience" -> "is just a bis audience".asJson)))
    }

    val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("something-else" -> "anything".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      Future.successful(Output(in))
    }

    val wf = (job1 && job1bis) --> job2

    val program = for {
      project <- FlowGraph("test05", "Test of jobs execution")(wf)
      browse  <- project.start().compile.toList
    } yield {
      browse.length shouldBe browse.count(_.isRight) // Everything went fine

      val sequenceOfJobs = browse.map(_.right.get)

      sequenceOfJobs.last should have size 0

      val job2Exec = sequenceOfJobs.dropRight(1).last

      job2Exec should have size 1 // One execution at the end

      val (job, ctx, result) = job2Exec.head

      result.isCompleted shouldBe true // Job has finished correctly

      val goodField = result.value.get match {
        case Success(done) => done match {
          case Output(json) =>
            json.hcursor.downField("something_else").as[String].isRight &&
              json.hcursor.downField("audience").downField("step_one").as[String].isRight
          case _ => false
        }
        case _ => false
      }

      goodField shouldBe true
    }


    program.unsafeRunSync()
  }
}
