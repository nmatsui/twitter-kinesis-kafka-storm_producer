package jp.co.tis.stc.example.kafka.producer

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

import java.util.Properties

class TweetStreamProducer(brokers:String, topic:String) {
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("request.required.acks", "-1")
  props.put("producer.type", "sync")
  props.put("clinet.id", topic)
  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def send(message:String):Unit = {
    producer.send(new KeyedMessage[AnyRef, AnyRef](topic, message.getBytes("UTF-8")))
  }
}
