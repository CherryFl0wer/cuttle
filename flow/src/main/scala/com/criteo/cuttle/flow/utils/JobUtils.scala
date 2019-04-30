package com.criteo.cuttle.flow.utils

object JobUtils {
  def failedJob(msg : String)  = scala.concurrent.Future.failed(new Exception(msg))
}
