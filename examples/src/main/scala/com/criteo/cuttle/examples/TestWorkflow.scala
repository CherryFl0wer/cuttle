package com.criteo.cuttle.examples

import java.time.{Instant, LocalDate}
import java.time.ZoneOffset.UTC


import com.criteo.cuttle._
import com.criteo.cuttle.flow._

import scala.concurrent.Future


object TestWorkflow {
  def main(args: Array[String]): Unit = {


    val start: Instant = LocalDate.now.atStartOfDay(UTC).toInstant
    val firstJob = Job("Step1", FlowScheduling(), "Data preparation") {
      implicit e =>

        e.streams.info("Testing 1")
        Future.successful(Completed)
    }

    val secondJob = Job("Step2", FlowScheduling(), "train") {
      implicit e =>

        e.streams.info("Testing 2")
        Future.successful(Completed)
    }

    val thirdJob = Job("Step3", FlowScheduling(), "predicting") {
      implicit e =>

        e.streams.info("Testing 3")
        Future.successful(Completed)
    }

    val fourthJob = Job("Step4", FlowScheduling(), "modeling") {
      implicit e =>

        e.streams.info("Testing 4")
        Future.successful(Completed)
    }

    val fifthJob = Job("Step5", FlowScheduling(), "Fooing") {
      implicit e =>

        e.streams.info("Testing 5")
        Future.successful(Completed)
    }

    val sixthJob = Job("Step6", FlowScheduling(), "Booing") {
      implicit e =>

        e.streams.info("Testing 6")
        Future.successful(Completed)
    }


    FlowProject("Testing prog", "1.0", "Testing code to implement flow workflow") {
      sixthJob dependsOn ( (fourthJob dependsOn (firstJob and secondJob)) and (fifthJob dependsOn thirdJob))
     //sixthJob dependsOn (fourthJob and fifthJob) dependsOn thirdJob dependsOn (firstJob and secondJob)
    }.start()
  }

}
