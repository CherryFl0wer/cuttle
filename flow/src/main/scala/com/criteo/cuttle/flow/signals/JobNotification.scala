package com.criteo.cuttle.flow.signals

import cats.effect.{Concurrent, ContextShift, IO}
import com.criteo.cuttle.flow.{FlowArg, FlowScheduling, NoArg}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object SignallingJob {
  import com.criteo.cuttle._

  /**
    * Method `kafka` is waiting (async) for `event` to come
    * once it is received will execute next job
    *
    * @param jobId job id
    * @param event event name
    * @param kafkaService Kafka notification class to manipulate a stream of notification
    * @param F Concurrent operation
    * @param cs Context shit IO
    * @return
    * @todo What to do when not consuming msg ?
    */
  def kafka(jobId : String, event : String, kafkaService : KafkaNotification[String, String])
           (implicit F : Concurrent[IO], cs : ContextShift[IO]): Job[FlowScheduling[NoArg, NoArg]] = {

    Job(jobId, s"waiting for event ${event}", FlowScheduling(NoArg(), NoArg()), SignalJob(event)) {
      implicit e =>

      val p = Promise[Unit]()

      e.streams.info(s"Waiting for event ${event} to be triggered in ${e.context.workflowId} by ${e.job.id}")
      kafkaService
        .subscribeOn(record => record.key == e.context.workflowId && record.value == event)
        .evalTap(msg => msg.committableOffset.commit)
        .head
        .compile
        .last
        .unsafeToFuture()
        .onComplete {
          cb => cb match {
            case Failure(err) => p.failure(new Exception(s"Job ${e.job.id} failed to complete correcty because ${err.getMessage}"))
            case Success(value) => p.success(())
          }
        }

      p.future.map(_ => Finished)
    }
  }


  /**
    * KafkaSE (Kafka Side Effect) will execute `effect` and asynchronously
    * wait to receive a notification. If the job finish first then it won't wait for the message
    * On the other hand if it receives a notification first will stop the execution
    *
    * @todo change it, because if the execution is writing on a disk you can't stop the exec like this
    *
    * @param jobId job id
    * @param event event name
    * @param kafkaService Kafka notification class to manipulate a stream of notification
    * @param effect Side effect method that consume a job execution and return completed once done
    *               This code is executed inside a Future
    * @param F Concurrent operation
    * @param cs Context shit IO
    * @return Job
    */

  def kafkaSE(jobId : String, event : String, kafkaService : KafkaNotification[String, String])
             (effect : Execution[FlowScheduling[_, _]] => Completed)
             (implicit F : Concurrent[IO], cs : ContextShift[IO]) = {

    Job(jobId, s"waiting for event ${event}", FlowScheduling(NoArg(), NoArg()), SignalJob(event)) { implicit e =>

      val kafkaPromise = Promise[Unit]()
      val sideEffectPromise = Promise[Unit]()

      e.streams.info(s"Waiting for event ${event} to be triggered in ${e.context.workflowId} by ${e.job.id}")

      val sideEffectFuture = Future { effect(e) }

      val kafkaFuture = kafkaService
        .subscribeOn(record => record.key == e.context.workflowId && record.value == event)
        .evalTap(msg => msg.committableOffset.commit)
        .head
        .compile
        .last
        .unsafeToFuture()

      sideEffectFuture.onComplete {
        cb => cb match {
          case Failure(err) => sideEffectPromise.failure(
            new Exception(s"Job ${e.job.id} failed to execute side effect function, reason is : ${err.getMessage}")
          )
          case Success(value) => sideEffectPromise.success(())
        }
      }

      kafkaFuture.onComplete {
          cb => cb match {
            case Failure(err) => kafkaPromise.failure(new Exception(s"Job ${e.job.id} failed to complete correctly because ${err.getMessage}"))
            case Success(value) => kafkaPromise.success(())
          }
      }

      Future
        .firstCompletedOf(List(kafkaPromise.future, sideEffectPromise.future))
        .map(_ => Finished)
    }
  }
}