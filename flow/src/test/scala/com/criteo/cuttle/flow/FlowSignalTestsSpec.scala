package com.criteo.cuttle.flow

import cats.effect.{ContextShift, IO, Timer}
import com.criteo.cuttle.Utils.logger
import com.criteo.cuttle.flow.FlowSchedulerUtils.WFSignalBuilder
import com.criteo.cuttle.flow.utils.KafkaConfig
import com.criteo.cuttle.{Execution, Finished, ITTestScheduling, Job, Output}
import fs2.Stream
import io.circe.Json
import org.scalatest.Matchers
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import io.circe.syntax._
import fs2._


/**
  * @summary Functionals tests of the sub-library of scheduling `Flow` placed in [[com.criteo.cuttle.flow._]]
  *          - Testing signals
  *
  *           PS: Job's are not sophisticated, they are simple,
  *           they are here to assure that execution is conform at what we expect
  *
  * */
class FlowSignalTestsSpec extends FunSuite with ITTestScheduling with Matchers {

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val job: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"job-${i.toString}", FlowScheduling())(completed))

  def waitingJob(time : FiniteDuration) : Job[FlowScheduling] = Job("job-waiting", FlowScheduling()) { implicit e =>
    e.park(time).map(_ => Finished)(ExecutionContext.global)
  }


  val failedSideEffect: (Execution[_]) => Future[Nothing] = (_ : Execution[_]) => Future.failed(new Exception("Failed task")) // Move to TestScheduling
  val failedJob: Vector[Job[FlowScheduling]] = Vector.tabulate(10)(i => Job(s"failed-${i.toString}", FlowScheduling())(failedSideEffect))

  test("Signal sync") {

    val simpleWorkflow : WFSignalBuilder[String, String] = topic => {

      val job1 = Job(s"step-one", FlowScheduling(inputs = Json.obj("audience" -> "step is one".asJson))) { implicit e =>
        IO(Finished).unsafeToFuture()
      }

      val job1bis = Job(s"step-one-bis", FlowScheduling()) { implicit e =>
        e.streams.info("On step bis")
        IO(Finished).unsafeToFuture()
       /* val receive = for {
          value <- topic
            .subscribeOn(msg => msg.key() == "wf-1" && msg.value() == "step-one")
            .head
            .compile
            .last
        } yield { Output(Json.obj("aud" -> "step bis is done".asJson)) }

        receive.unsafeToFuture()*/
      }

      val job2 = Job(s"step-two", FlowScheduling(inputs = Json.obj("test" -> "final test".asJson))) { implicit e =>
        val in = e.job.scheduling.inputs
        val x = e.optic.aud.string.getOption(in).get + " passed to step three"
        IO(Output(x.asJson)).unsafeToFuture()
      }

      (job1 && job1bis) --> job2
    }


    val program = for {
      // Initialisation
      signalManager   <- Stream.eval(KafkaMessage[String, String](KafkaConfig("signal_cuttle", "signals", List("localhost:9092"))))
      // Setup Workflow
      _ <- Stream(()).concurrently(signalManager.consumeFromKafka)
      workflowWithTopic = simpleWorkflow(signalManager)
      project <- Stream.eval(FlowGraph("test-signal-01", "Test of jobs signal")(workflowWithTopic))
      // Run it
      res <- project.start()
                    .concurrently(signalManager
                      .pushOne("wf-1", "step-one")
                      .delayBy[IO](3.seconds)
                      .map(_ => ())
                    )
    } yield res

    val programExecutions = program.compile.toList.unsafeRunSync()
    programExecutions.length shouldBe 4

  }


  /*
  test("Job signal on simple workflow `success only job`") {

    val flowTestSignalTopic = new KafkaNotification[String, String](KafkaConfig(
      topic = "test-signal",
      groupId = "flow-signal-test-consumer",
      servers = List("localhost:9092")))

    val signalJ = SignallingJob.kafka("job-signal-00-test", "signal-00-test", flowTestSignalTopic)

    val wf = (job(0) && signalJ) --> job(1) --> job(2)

    val project = FlowGraph("test03", "Test of jobs signals")(wf)

    val toPush = flowTestSignalTopic.pushOne((project.workflowId, "signal-00-test"))

    val pusher = fs2.Stream.awakeEvery[IO](5.seconds).head.flatMap(_ => toPush)

    val actions = project.start()

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
      runnedJobs should be ('right)
      runnedJobs.right.get.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
      x += 1
    }
  }


  test("Multiple signals on a more complex workflow `success only job`") {

    import fs2.Stream

    val flowTestSignalTopic = new KafkaNotification[String, String](KafkaConfig(
      topic = "test-signal",
      groupId = "flow-signal-test-consumer",
      servers = List("localhost:9092")))

    val signalJ1 = SignallingJob.kafka("job-3-signal-test", "signal-03-test", flowTestSignalTopic)
    val signalJ2 = SignallingJob.kafka("job-6-signal-test", "signal-06-test", flowTestSignalTopic)

    val wf = job(0) --> (job(1) && job(2)) --> signalJ1 --> (job(4) && job(5)) --> (job(6) && signalJ2) --> job(7)

    val project = FlowGraph("test04", "Test of jobs signals")(wf)

    val pusher = for {
      _ <- Stream.awakeEvery[IO](5.seconds).head
      firstSig <- flowTestSignalTopic.pushOne((project.workflowId, "signal-03-test"))
      _ <- Stream.awakeEvery[IO](5.seconds).head
      secondSig <- flowTestSignalTopic.pushOne((project.workflowId, "signal-06-test"))
    } yield (firstSig, secondSig)

    val actions = project.start()

    val done = actions.concurrently(pusher).concurrently(flowTestSignalTopic.consume)

    val res = done.compile.toList.unsafeRunSync()

    val testingList = List(
      List("job-0"),
      List("job-1", "job-2"),
      List("job-3-signal-test"),
      List("job-4", "job-5"),
      List("job-5"),
      List("job-6", "job-6-signal-test"),
      List("job-6-signal-test"),
      List("job-7"),
      List.empty
    )

    var x = 0
    res.foreach { runnedJobs =>
      runnedJobs should be ('right)
      runnedJobs.right.get.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
      x += 1
    }
  }


  test("Multiple signals on a more complex workflow with errors") {

    import fs2.Stream

    val flowTestSignalTopic = new KafkaNotification[String, String](KafkaConfig(
      topic = "test-signal",
      groupId = "flow-signal-test-consumer",
      servers = List("localhost:9092")))

    val signalJ1 = SignallingJob.kafka("job-3-signal-test", "signal-03-test", flowTestSignalTopic)
    val signalJ2 = SignallingJob.kafka("job-6-signal-test", "signal-06-test", flowTestSignalTopic)
    val errorJob = job(9)

    // it is possible to catch error on a signal because signal job return an exception
    val wf = job(0) --> (job(1) && job(2)) --> signalJ1.error(errorJob) --> (job(4) && job(5)) --> (job(6) && signalJ2) --> job(7)

    val project = FlowGraph("test04", "Test of jobs signals")(wf)

    val pusher = for {
      _ <- Stream.awakeEvery[IO](5.seconds).head
      firstSig <- flowTestSignalTopic.pushOne((project.workflowId, "signal-03-test"))
      _ <- Stream.awakeEvery[IO](5.seconds).head
      secondSig <- flowTestSignalTopic.pushOne((project.workflowId, "signal-06-test"))
    } yield (firstSig, secondSig)

    val actions = project.start()

    val done = actions.concurrently(pusher).concurrently(flowTestSignalTopic.consume)

    val res = done.compile.toList.unsafeRunSync()

    val testingList = List(
      List("job-0"),
      List("job-1", "job-2"),
      List("job-3-signal-test"),
      List("job-4", "job-5"),
      List("job-5"),
      List("job-6", "job-6-signal-test"),
      List("job-6-signal-test"),
      List("job-7"),
      List.empty
    )

    var x = 0
    res.foreach { runnedJobs =>
      runnedJobs should be ('right)
      runnedJobs.right.get.toList.map(_._1.id) should contain theSameElementsAs testingList(x)
      x += 1
    }
  }
*/
}


