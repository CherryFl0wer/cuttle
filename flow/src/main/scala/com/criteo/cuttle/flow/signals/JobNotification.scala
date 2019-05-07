package com.criteo.cuttle.flow.signals

import cats.effect.{Async, Concurrent, ContextShift, IO}
import com.criteo.cuttle.flow.FlowScheduling

import scala.concurrent.Promise
import scala.util.{Failure, Success}

object SignallingJob {
  import com.criteo.cuttle._

  /**
    * @todo Routing: success or error
    * @todo Decide which job will be executed next
    * @todo What to do when not consuming msg ?
    * @todo Failure test
    */
  def kafka(jobId : String, event : String, kafkaService : KafkaNotification[String, String])
                      (implicit F : Concurrent[IO], cs : ContextShift[IO]): Job[FlowScheduling] = {

    Job(jobId, FlowScheduling(), s"waiting for event ${event}", SignalJob(event)) { implicit e =>

      val p = Promise[Unit]()

      e.streams.info(s"Waiting for event ${event} to be triggered in ${e.context.workflowId} by ${e.job.id}")
      kafkaService
        .subscribeOn(record => record.key == e.context.workflowId && record.value == event)
        .evalTap(msg => msg.committableOffset.commit)
        .head
        .compile
        .last
        .unsafeToFuture()
        .onComplete { cb => cb match {
            case Failure(err) => p.failure(new Exception(s"Job ${e.job.id} failed to complete correcty because ${err.getMessage}"))
            case Success(value) =>  {
              p.success(())
            }
          }
        }

      p.future.map(_ => Finished)
    }
  }
}