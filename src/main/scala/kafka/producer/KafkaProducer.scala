package jp.co.tis.stc.example.kafka.producer

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

import scala.collection.JavaConverters._

import jp.co.tis.stc.example.IStreamProducer
import jp.co.tis.stc.example.util.PropertyLoader

class KafkaProducer() extends IStreamProducer with PropertyLoader {
  private val kafkaConf = loadProperties("kafka.properties")

  private val props = new java.util.Properties()
  props.put("metadata.broker.list", kafkaConf("kafka.brokers"))
  props.put("request.required.acks", "-1")
  props.put("producer.type", "sync")
  props.put("clinet.id", kafkaConf("kafka.topic"))
  private val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def send(message:String):Unit = {
    producer.send(new KeyedMessage[AnyRef, AnyRef](kafkaConf("kafka.topic"), message.getBytes("UTF-8")))
  }
}
