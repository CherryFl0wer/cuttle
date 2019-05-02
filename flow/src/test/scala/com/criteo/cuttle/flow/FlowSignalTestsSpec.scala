package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import com.criteo.cuttle.flow.signals.{KafkaConfig, KafkaNotification, SignallingJob}
import com.criteo.cuttle.{Completed, Job, TestScheduling}
import org.scalatest.Matchers
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration._

/**
  * @summary Functionals tests of the sub-library of scheduling `Flow` situated in [com.criteo.cuttle.flow._]
  *          - Testing signals
  *
  *           PS: Job's are not sophisticated, they are simple,
  *           they are here to assure that execution is conform at what we expect
  *
  * @Todo Own database for the tests
  * */
class FlowSignalTestsSpec extends FunSuite with TestScheduling with Matchers {

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(6)(i => Job(s"job-${i.toString}", FlowScheduling())(completed))

  def waitingJob(time : FiniteDuration) : Job[FlowScheduling] = Job("job-waiting", FlowScheduling()) { implicit e =>
    e.park(time).map(_ => Completed)(ExecutionContext.global)
  }




  test("Signal sync") {
    val flowTestSignalTopic = new KafkaNotification[String, String](KafkaConfig(
      topic = "test-signal-01",
      groupId = "flow-signal-test-consumer",
      servers = List("localhost:9092")))

    val toPush = flowTestSignalTopic.pushOne(("test01-signal", "1")) ++ flowTestSignalTopic.pushOne(("test01-signal", "2")) ++ flowTestSignalTopic.pushOne(("test01-signal", "3"))
    val msgSent = toPush.compile.toList.unsafeRunSync()

    val consumed = flowTestSignalTopic
      .consumeAll
      .evalTap(msg => msg.committableOffset.commit)
      .interruptAfter(5 seconds)
      .compile
      .toList
      .unsafeRunSync()


    consumed.size should be equals msgSent.size

    var x = 0;
    consumed.foreach { msg =>
      msg.record.key should be equals msgSent(x).records._1.key
      msg.record.value() should be equals msgSent(x).records._1.value
      x += 1
    }
  }

  test("Job signal") {

    val flowTestSignalTopic = new KafkaNotification[String, String](KafkaConfig(
      topic = "test-signal",
      groupId = "flow-signal-test-consumer",
      servers = List("localhost:9092")))

    val signalJ = SignallingJob.kafka("job-signal-00-test", "signal-00-test", flowTestSignalTopic)

    val wf = (job(0) && signalJ) --> job(1) --> job(2)

    val project = FlowProject("test03", "Test of jobs signals")(wf)

    val toPush = flowTestSignalTopic.pushOne((project.workflowId, "signal-00-test"))

    val pusher = fs2.Stream.awakeEvery[IO](5 seconds).head.flatMap(_ => toPush)

    val actions = project.start[IO]()

    val done = actions.concurrently(pusher).concurrently(flowTestSignalTopic.consume)

    val res = done.compile.toList.unsafeRunSync()

    val testingList = List(
      List("job-0", "job-signal-00-test"),
      List("job-signal-00-test"),
      List("job-1"),
      List("job-2"),
      List.empty
    )

    var x = 0
    res.foreach { runnedJobs =>
      runnedJobs.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
      x += 1
    }
  }
}

