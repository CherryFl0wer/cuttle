package com.criteo.cuttle.flow.signals

import java.util.Properties

/**
  * Use configuration library: pure config, play config, etc
 *
  * @todo make it possible to use different serializers.
  * @todo introduce type parameter.
  */
case class KafkaConfig(groupId: String, servers: List[String]) {
  private val keySerializer = "org.apache.kafka.common.serialization.StringSerializer"
  private val valueSerializer = "org.apache.kafka.common.serialization.StringSerializer"
  private val keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
  private val valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"

  def producerProperties: Properties = {
    val properties = new Properties()
    properties.put("key.serializer", keySerializer)
    properties.put("value.serializer", valueSerializer)
    properties.put("bootstrap.servers", servers.mkString(","))
    properties
  }
  def consumerProperties: Properties = {
    val properties = new Properties()
    properties.put("key.deserializer", keyDeserializer)
    properties.put("value.deserializer", valueDeserializer)
    properties.put("bootstrap.servers", servers.mkString(","))
    properties.put("group.id", groupId)
    properties
  }
}
