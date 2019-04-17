package com.criteo.cuttle.flow.signals

import cats.effect.{Async, Concurrent, ExitCode, IO}
import com.criteo.cuttle.flow.FlowScheduling

import scala.async.Async.{async, await}
import scala.concurrent.{Future, Promise}

trait JobNotification {}


object SignallingJob {
  import com.criteo.cuttle._

  /**
    * @todo Routing: success or error
    * @todo Decide which job will be executed next
    * @todo What to do when not consuming msg ?
    */
  def kafkaSignaledJob(jobId : String, event : String, kafkaService : KafkaNotification[String, String])
                      (implicit F : Concurrent[IO]): Job[FlowScheduling] = {

    Job(jobId, FlowScheduling(), s"waiting for event ${event}", SignalJob(event)) { implicit e =>

      // If wfid and signal is in Then Fill the promise of the job with successfull otherwise ?

      // val jobEvent = e.job.kind.asInstanceOf[SignalJob].eventTrigger // not necessary in this case

      val p = Promise[Completed]()

      e.streams.info(s"Waiting for event ${event} to be triggered in ${e.context.workflowId} by ${e.job.id}")

      kafkaService
        .subscribeTo(record => record.key == e.context.workflowId && record.value == event)
        .head
        .compile
        .last
        .unsafeToFuture()
        .onComplete { cb =>
          cb.toOption match {
            case None => sys.error("Failed job") // TODO : Not that
            case Some(commit) => for {
              _ <- kafkaService.pushCommit(commit.get)
              _ <- p.success(Completed)
            } yield ()
          }
      }

      p.future
    }
  }
}