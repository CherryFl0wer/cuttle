package com.criteo.cuttle

import cats.effect.IO

package object examples {

  /** Default implicit logger that output everything to __stdout__ */
  implicit val logger = new Logger {
    def logMe(message: => String, level: String) = IO(println(s"${java.time.Instant.now}\t${level}\t${message}"))
    override def info(message: => String): IO[Unit] = logMe(message, "INFO")
    override def debug(message: => String): IO[Unit] = logMe(message, "DEBUG")
    override def warn(message: => String): IO[Unit] = logMe(message, "WARN")
    override def error(message: => String): IO[Unit] = logMe(message, "ERROR")
    override def trace(message: => String): IO[Unit] = IO.unit
  }
}
