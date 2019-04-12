package com.criteo.cuttle.flow.signals

import cats.effect.IO
import com.criteo.cuttle.flow.FlowScheduling

import scala.concurrent.{Await, Future}

trait JobNotification {}


object SignallingJob {
  import com.criteo.cuttle._

  /**
    * @todo Routing: success or error
    * @todo Decide which job will be executed next
    * @todo What to do when not consuming msg ?
    */
  def kafkaSignaledJob(jobId : String, description : String, kafkaService : KafkaNotification[String, String]) : Job[FlowScheduling] = {
    Job(jobId, FlowScheduling(), description, SignalJob("declare-step")) { implicit e =>
      // here receive a stream of message
      // filter it
      // If wfid and signal is in Then Fill the promise of the job with successfull otherwise ?
      Future.successful(Completed)
    }
  }
}