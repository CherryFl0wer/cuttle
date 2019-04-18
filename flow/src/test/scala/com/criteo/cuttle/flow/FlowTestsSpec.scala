package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification}
import com.criteo.cuttle.{Job, TestScheduling}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext

class FlowTestsSpec extends FunSuite with TestScheduling {


  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(4)(i => Job(i.toString, FlowScheduling())(completed))
  val signalJob = new KafkaNotification[String, String](KafkaConfig(
    topic = "signal-flow-test",
    groupId = "flow-signal",
    servers = List("localhost:9092")))

  val scheduler = FlowScheduler(logger, "workflow-from-test")

  test("it should validate empty workflow") {
    val workflow = FlowWorkflow.empty

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate unit workflow") {
    val workflow = job(0)

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate workflow without cycles and valid start dates") {
    val workflow = job(0) dependsOn job(1) dependsOn job(2)

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate workflow without cycles (one parent with many children)") {
    val job1: Vector[Job[FlowScheduling]] =
      Vector.tabulate(10)(i => Job(java.util.UUID.randomUUID.toString, FlowScheduling())(completed))
    val workflow = (0 to 8).map(i => job1(i) dependsOn job1(9)).reduce(_ and _)

    assert(FlowSchedulerUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it shouldn't validate cyclic workflow") {
    val workflow = job(0) dependsOn job(1) dependsOn job(2) dependsOn job(0)

    assert(FlowSchedulerUtils.validate(workflow).isLeft, "workflow passed a validation of cycle presence")
  }


  /*

    val words = List("V1", "V2", "V3")
    signalJob
      .consume
      .compile
      .drain
      .unsafeRunAsyncAndForget()

    words.foreach(sig => signalJob.pushOne((project.workflowId, sig)).compile.drain.unsafeRunSync())
  */
  test("it should ") {
    val wf = job(0) <-- job(1) <-- (job(2) :: job(3))
    val project = FlowProject("test", "Test of jobs execution")(wf)

    project.start().unsafeRunSync()
  }
}
