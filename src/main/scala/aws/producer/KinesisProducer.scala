package jp.co.tis.stc.example.aws.producer

import java.nio.ByteBuffer

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{ PutRecordRequest, PutRecordResult }

import scala.collection.JavaConverters._

import jp.co.tis.stc.example.IStreamProducer

class KinesisProducer() extends IStreamProducer {
  private val prop = new java.util.Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("kinesis.properties"))
  private val conf = prop.asScala

  private val clientConfig = new ClientConfiguration()
  conf.get("http.proxyHost").map(k=>clientConfig.setProxyHost(k))
  conf.get("http.proxyPort").map(k=>clientConfig.setProxyPort(k.toInt))
  conf.get("http.proxyUser").map(k=>clientConfig.setProxyUsername(k))
  conf.get("http.proxyPassword").map(k=>clientConfig.setProxyPassword(k))

  private val client = new AmazonKinesisClient(new ClasspathPropertiesFileCredentialsProvider(), clientConfig)

  def send(message:String):Unit = {
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(conf("kinesis.streamName"))
    putRecordRequest.setData(ByteBuffer.wrap(message.getBytes("UTF-8")))
    putRecordRequest.setPartitionKey("key")
    val putRecordResult = client.putRecord(putRecordRequest)
  }
}
