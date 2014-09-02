package jp.co.tis.stc.example

import jp.co.tis.stc.example.kafka.producer.KafkaProducer
import jp.co.tis.stc.example.aws.producer.KinesisProducer

trait IStreamProducer {
  def send(message:String):Unit
}

object StreamProducerFactory {
  def getInstance(producerType:String):IStreamProducer = producerType match {
    case "TEST" => {
      new IStreamProducer {
        def send(message:String):Unit = {}//Nothing to do
      }
    }
    case "KAFKA" => new KafkaProducer()
    case "KINESIS" => new KinesisProducer()
    case _ => throw new RuntimeException
  }
}
