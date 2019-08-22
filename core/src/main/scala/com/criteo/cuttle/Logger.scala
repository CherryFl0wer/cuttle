package com.criteo.cuttle

import cats.effect.IO

/** A logger used to output internal informations. */
trait Logger {
  def debug(message: => String): IO[Unit] = IO.unit
  def info(message: => String): IO[Unit] = IO.unit
  def warn(message: => String): IO[Unit] = IO.unit
  def error(message: => String): IO[Unit] = IO.unit
  def trace(message: => String): IO[Unit] = IO.unit
}