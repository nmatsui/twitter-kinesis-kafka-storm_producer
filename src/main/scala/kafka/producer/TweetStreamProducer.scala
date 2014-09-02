package jp.co.tis.stc.example.kafka.producer

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

import scala.collection.JavaConverters._

import jp.co.tis.stc.example.IStreamProducer

class TweetStreamProducer() extends IStreamProducer {
  private val prop = new java.util.Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("kafka.properties"))
  private val conf = prop.asScala

  private val props = new java.util.Properties()
  props.put("metadata.broker.list", conf("kafka.brokers"))
  props.put("request.required.acks", "-1")
  props.put("producer.type", "sync")
  props.put("clinet.id", conf("kafka.topic"))
  private val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def send(message:String):Unit = {
    producer.send(new KeyedMessage[AnyRef, AnyRef](conf("kafka.topic"), message.getBytes("UTF-8")))
  }
}
