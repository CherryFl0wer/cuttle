package com.criteo.cuttle.flow

import cats.implicits._
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Timer}
import fs2.concurrent.Queue
import fs2.Stream

import scala.concurrent.duration._

class Buffering[F[_]](q2: Queue[F, Int])(implicit F: Concurrent[F]) {

  def start = q2.dequeue.evalMap(n => F.delay(println(s"Pulling out $n from Queue #2")))


  def push(x : Int*) = Stream.emits(x).through(q2.enqueue)
}

object Fifo extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      q2 <- Stream.eval(Queue.bounded[IO, Int](10))
      bp = new Buffering[IO](q2)
      _  <- Stream(
        bp.start.drain,
        Stream.awakeEvery[IO](2.seconds).flatMap(_ => bp.push(1, 2, 3, 4)).drain
      ).parJoin(2)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}