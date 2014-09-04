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
  conf.get("http.proxyHost").filter(_.nonEmpty).map(v=>clientConfig.setProxyHost(v))
  conf.get("http.proxyPort").filter(_.nonEmpty).map(v=>clientConfig.setProxyPort(v.toInt))
  conf.get("http.proxyUser").filter(_.nonEmpty).map(v=>clientConfig.setProxyUsername(v))
  conf.get("http.proxyPassword").filter(_.nonEmpty).map(v=>clientConfig.setProxyPassword(v))

  private val client = new AmazonKinesisClient(new ClasspathPropertiesFileCredentialsProvider(), clientConfig)

  def send(message:String):Unit = {
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(conf("kinesis.streamName"))
    putRecordRequest.setData(ByteBuffer.wrap(message.getBytes("UTF-8")))
    putRecordRequest.setPartitionKey("key")
    val putRecordResult = client.putRecord(putRecordRequest)
  }
}
