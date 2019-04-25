package com.criteo.cuttle.examples

import cats.implicits._
import cats.effect._
import fs2._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


/**
  * @summary This file is here to test approach of some utils method through FS2.
  *          TestFS2 is therefore put inside a test folder of `com.criteo.cuttle.examples`
  *          It is just test of test.
  * */
object TestFs2 extends IOApp{

  implicit val cs = ExecutionContext.global

  def tailling[F[_]](s : Set[Future[Int]])(implicit F: Concurrent[F], S : Sync[F]): F[Set[Future[Int]]] = F.async {
    cb =>
      val ft = Future.firstCompletedOf(s)

      ft.onComplete {
          case Failure(exception) => cb(Left(exception))
          case Success(value) => {
            cb(Right(s.filterNot(_.isCompleted)))
          }
      }

  }

  def doIn(time : FiniteDuration, work : IO[Unit]) = Stream.awakeEvery[IO](time).head *> Stream.eval_(work)


  def trampoline[F[_], O](call : O => F[O], over : O => Boolean) : Pipe[F, O, O] = {

    def go(s: Stream[F,O]): Pull[F,O,Unit] = {
      s.pull.uncons.flatMap {
        case Some((hd,tl)) =>
          hd.size match {
            case m if m >= 1 => { // Whatever the size is we want the head
              val elm = hd.head.get
              Pull.output(hd.take(1)) >> (
                if (over(elm)) Pull.done
                else go(Stream.eval(call(elm)))
                )
            }
            case 0 => Pull.done // No element in chunks then done
          }
        case None => Pull.done
      }
    }
    in => go(in).stream
  }

  override def run(args: List[String]): IO[ExitCode] = {

    val streamOfFuture = Stream(Set(
      Future.successful(1),
      Future { Thread.sleep(7000); 2 },
      Future { Thread.sleep(3000); 3 },
      Future.successful(4)))

    val res = streamOfFuture
        .covary[IO]
        .through(trampoline(tailling[IO], (x : Set[Future[Int]]) => x.isEmpty))

    val done = res.compile.toList.unsafeRunSync
    done.foreach(p => println(p))

    IO(ExitCode.Success)
  }
}
