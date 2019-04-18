package com.criteo.cuttle.examples

import cats.effect.{ExitCode, IO, IOApp}
import scala.concurrent.duration._
import cats.implicits._
import fs2.Stream

object TestFs2 extends IOApp{
  override def run(args: List[String]): IO[ExitCode] = {

    // Test wait x seconds and print
    val stream = Stream.awakeEvery[IO](3.seconds).head *> Stream.eval_(IO.delay(println("ok")))
    stream.compile.drain.unsafeRunSync()



    IO(ExitCode.Success)
  }
}
