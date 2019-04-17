package com.criteo.cuttle.flow.signals

import cats.effect.{ Concurrent, IO}
import com.criteo.cuttle.flow.FlowScheduling

import scala.concurrent.Promise

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

      val p = Promise[Unit]()

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
            case Some(commit) =>  {
              kafkaService.pushCommit(commit.get).unsafeRunSync() // In his own context
              p.success(())
            }
          }
      }

      e.parkWhen(p.future).map(_ => Completed)
    }
  }
}