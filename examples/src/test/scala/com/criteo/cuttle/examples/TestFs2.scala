package com.criteo.cuttle.examples

import cats.effect.{ExitCode, IO, IOApp}
import scala.concurrent.duration._
import cats.implicits._

object TestFs2 extends IOApp{
  override def run(args: List[String]): IO[ExitCode] = {

    // Test wait x seconds and print
    val stream = fs2.Stream.awakeEvery[IO](3.seconds).head *> fs2.Stream.eval_(IO.delay(println("ok")))
    stream.compile.drain.unsafeRunSync()


    IO(ExitCode.Success)
  }
}
