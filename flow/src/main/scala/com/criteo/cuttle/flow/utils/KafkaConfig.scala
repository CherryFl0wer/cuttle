package com.criteo.cuttle.flow.utils

import scala.concurrent.duration._

/**
  * Use configuration library: pure config, play config, etc
 *
  * @todo introduce type parameter.
  */
case class KafkaConfig(topic: String,
                       groupId: String,
                       servers: List[String],
                       offsetInterval : (Int, FiniteDuration) = (500, 15 seconds)) {

  def serversToString = servers.mkString(",")
}
