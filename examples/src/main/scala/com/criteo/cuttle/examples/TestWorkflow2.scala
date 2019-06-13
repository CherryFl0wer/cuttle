package com.criteo.cuttle.examples

import java.time.Instant

import cats.effect._
import com.criteo.cuttle._
import com.criteo.cuttle.flow.FlowSchedulerUtils.FlowJob
import com.criteo.cuttle.flow._

import scala.concurrent.Future


/**
  * The goal of this test is to get to work the routing system
  */

object TestWorkflow2 extends IOApp {

  case class DayJobInput(dayStart : String, dayEnd : String) extends FlowArg

  implicit val dayJobInputDecoder = io.circe.generic.semiauto.deriveDecoder[DayJobInput]
  implicit val dayJobInputEncoder = io.circe.generic.semiauto.deriveEncoder[DayJobInput]

  def run(args: List[String]): IO[ExitCode] = {

    val run = FlowProject()(dayJob).start()

    val list = run.compile.toList.unsafeRunSync

    list.foreach(p => println(p))

    IO(ExitCode.Success)
  }

  private val dayJob = Job("Step-End", "daying process", scheduling = FlowScheduling(DayJobInput("1", "2"), NoArg())) {
      implicit e =>

        e.streams.info("Running day process ")

        Future.successful(Finished)
  }

}
