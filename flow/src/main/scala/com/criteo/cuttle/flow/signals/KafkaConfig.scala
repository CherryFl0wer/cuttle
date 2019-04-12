package com.criteo.cuttle.flow.signals

/**
  * Use configuration library: pure config, play config, etc
 *
  * @todo introduce type parameter.
  */
case class KafkaConfig(topic: String, groupId: String, servers: List[String]) {
  def serversToString = servers.mkString(",")
}
