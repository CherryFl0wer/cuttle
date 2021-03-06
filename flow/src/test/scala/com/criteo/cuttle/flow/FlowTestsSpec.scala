package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.{DatabaseConfig, Execution, Finished, ITTestScheduling, Job, Output, OutputErr, Database => CoreDB}

import scala.concurrent.duration._
import org.scalatest._
import org.scalatest.BeforeAndAfter
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
class FlowTestsSpec extends FunSuite with ITTestScheduling with Matchers with BeforeAndAfter {


  implicit val ctx = ExecutionContext.global
  implicit val timer: Timer[IO] = IO.timer(ctx)
  implicit val cs: ContextShift[IO] = IO.contextShift(ctx)

  val xa = CoreDB.connect(DatabaseConfig.fromEnv)(logger)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"job-${i.toString}", FlowScheduling())(completed))

  val failedSideEffect: Execution[_] => Future[Nothing] = (_ : Execution[_]) => Future.failed(new Exception("Failed task"))
  val failedJob: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"failed-${i.toString}", FlowScheduling())(failedSideEffect))

  def waitingJob(id: String = "0", time : FiniteDuration) : Job[FlowScheduling] =
    Job(s"job-waiting-$id", FlowScheduling()) { implicit e =>
      e.park(time).map(_ => Finished)
    }


  before {
    import com.criteo.cuttle.{Database => CoreDB}
    import doobie.implicits._
    import com.criteo.cuttle.flow.{Database => FlowDB}

    (for {
      transactor <- xa
      _ <- CoreDB.doSchemaUpdates.transact(transactor)
      _ <- FlowDB.doSchemaUpdates.transact(transactor)
    } yield ()).unsafeRunSync()
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
    val program = for {
      tr <- xa
      project <- FlowExecutor(tr)(wf)
      graphFinal  <- project.start
    } yield {
      graphFinal should be ('right)
      val (flow, state) = graphFinal.toOption.get
      state.keySet should contain theSameElementsAs (flow verticesFrom RoutingKind.Success)
      state.values.toList should contain theSameElementsAs List(Done, Done, Done, Done)
    }
    program.unsafeRunSync()
  }

  test("it should execute sequential `and` (success only)") {
    val wf = job(0) --> (job(1) && job(2)) --> (job(3) && job(4))

    val program = for {
      tr <- xa
      project <- FlowExecutor(tr)(wf)
      graphFinal  <- project.start
    } yield {
      graphFinal should be ('right)
      val (_, state) = graphFinal.toOption.get
      state.keySet.map(_.id) should contain theSameElementsAs Set("job-0", "job-1", "job-2", "job-3", "job-4")
      state.values.toList should contain theSameElementsAs List(Done, Done, Done, Done, Done)
    }
    program.unsafeRunSync()

  }

  test("it should execute in order with multiple waiting job (success only) ") {
    val wf = (job(0) && job(1) && waitingJob("0",3.seconds)) --> job(2) --> (job(3) && waitingJob("1", 5.seconds)) --> job(4) --> job(5)

    val program = for {
      tr <- xa
      project <- FlowExecutor(tr)(wf)
      graphFinal  <- project.start
    } yield {
      graphFinal should be ('right)
      val (flow, state) = graphFinal.toOption.get
      state.keySet should contain theSameElementsAs(flow verticesFrom RoutingKind.Success)
      state.values.toList should contain theSameElementsAs List.fill(flow.vertices.size)(Done)
    }

    program.unsafeRunSync()
  }

  test("it should execute a tree like form (success only)") {

    val part1 = (job(0) && waitingJob(time = 3.seconds)) --> job(3)
    val part2 = job(2) --> job(4)
    val wf = (part1 && part2) --> job(5)

    val program = for {
      tr <- xa
      project <- FlowExecutor(tr)(wf)
      graphFinal  <- project.start
    } yield {
      graphFinal should be ('right)
      val (flow, state) = graphFinal.toOption.get
      state.keySet should contain theSameElementsAs(flow verticesFrom RoutingKind.Success)
      state.values.toList should contain theSameElementsAs List.fill(flow.vertices.size)(Done)
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
      tr <- xa
      project <- FlowExecutor(tr)(wf)
      graphFinal  <- project.start
    } yield {
      graphFinal should be ('right)
      val (flow, state) = graphFinal.toOption.get

      state.keySet should contain theSameElementsAs(flow verticesFrom RoutingKind.Success)
      state.values.toList should contain theSameElementsAs List.fill(flow.vertices.size)(Done)

      val stepTwo = flow get "step-two"
      stepTwo shouldBe defined

      val outStepTwo = stepTwo.get.scheduling.outputs
      val result = outStepTwo.hcursor.downField("something_else").as[String].isRight &&
        outStepTwo.hcursor.downField("audience").downField("step_one").as[String].isRight
      result shouldBe true
    }


    program.unsafeRunSync()
  }

  test("Should rerun a single job") {
    val wf = job(0) --> job(1)
    val program = for {
      tr <- xa
      project     <- FlowExecutor(tr)(wf)
      graphFinal  <- project.start

      job0 <- project.runSingleJob("job-0")

    } yield {
      graphFinal should be ('right)
      job0 should be ('right)

      val (flow, state) = graphFinal.right.get
      state.keySet should contain theSameElementsAs(flow verticesFrom RoutingKind.Success)
      state.values.toList should contain theSameElementsAs List.fill(flow.vertices.size)(Done)

      val (_, stateJob0) = job0.right.get
      stateJob0.keySet should contain theSameElementsAs Set(job(0))
      stateJob0.values should contain theSameElementsAs Set(Done)
    }

    program.unsafeRunSync()
  }

  test("Should trigger error in job and run both error job and then stop with Left") {
    import io.circe.syntax._

    import com.criteo.cuttle._

    lazy val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get  + " will pass to step two"
      throw new IndexOutOfBoundsException("No reason")
      Future.successful(Output(Json.obj("audience" -> x.asJson)))
    }

    val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
      Future.successful(OutputErr(Json.obj("audience" -> "is just a bis audience".asJson)))
    }

    val error = Job(s"Managing error", FlowScheduling()) { implicit e =>
      e.streams.error("Something went wrong")
      Future(Output(Json.obj("error" -> "error triggered".asJson)))
    }

    val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("something-else" -> "anything".asJson))) { implicit e =>
      val in = e.job.scheduling.inputs
      Future.successful(Output(in))
    }

    val wf = (job1.error(error) && job1bis.error(error)) --> job2

    val program = for {
      transactor <- xa
      project <- FlowExecutor(transactor)(wf)
      graphFinal  <- project.start
    } yield {
      graphFinal should be ('left)
    }

    program.unsafeRunSync()
  }

  test("it should rerun the workflow where it fails") {

    import io.circe.syntax._

    val jobfinal = Job(s"final", FlowScheduling()) { implicit e =>
      val x = e.optic.audience.string.getOption(e.job.scheduling.inputs).get
      Future.successful(Finished)
    }

    val part1 = job(0) --> job(3)
    val part2 = job(2) --> job(4)
    val wf = (part1 && part2) --> jobfinal

    val program = for {
      tr <- xa

      project <- FlowExecutor(tr)(wf)
      firstRun  <- project.start
      // ...  test if first run fail ...
      firstRunWorkflow <- project.workflowRef.get
      finalJob = firstRunWorkflow.get("final").get
      newFinalJob = finalJob.copy(scheduling = FlowScheduling(inputs = Json.obj("audience" -> "done".asJson)))(finalJob.effect)
      sndWorkflow = firstRunWorkflow.replace(newFinalJob)
      _ <- project.workflowRef.update(_ => sndWorkflow)
      secondRun <- project.start
    } yield {
      firstRun should be ('left)

      secondRun should be ('right)
      val (flow, state) = secondRun.toOption.get
      state.keySet should contain theSameElementsAs(flow verticesFrom RoutingKind.Success)
      state.values.toList should contain theSameElementsAs List.fill(flow.vertices.size)(Done)
    }

    program.unsafeRunSync()
  }
}
