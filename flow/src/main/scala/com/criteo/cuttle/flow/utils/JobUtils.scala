package com.criteo.cuttle.flow.utils

object JobUtils {
  // Return a failed future with message
  def failedJob(msg : String)  = scala.concurrent.Future.failed(new Exception(msg))

  // Replace with underscore
  def formatName(str : String) = str.replaceAll("(\\s|\\.|\\-|\\*|\\+|\\?|\\$|\\^|\\/|\\\\)+", "_")
}
