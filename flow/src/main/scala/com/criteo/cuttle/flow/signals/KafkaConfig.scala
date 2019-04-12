package com.criteo.cuttle.flow.signals

/**
  * Use configuration library: pure config, play config, etc
 *
  * @todo make it possible to use different serializers.
  * @todo introduce type parameter.
  */
case class KafkaConfig(groupId: String, servers: List[String]) {
  def serversToString = servers.mkString(",")
}
