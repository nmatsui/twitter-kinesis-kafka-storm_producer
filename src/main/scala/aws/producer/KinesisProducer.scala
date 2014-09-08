package jp.co.tis.stc.example.aws.producer

import java.nio.ByteBuffer

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{ PutRecordRequest, PutRecordResult }

import scala.collection.JavaConverters._

import jp.co.tis.stc.example.IStreamProducer
import jp.co.tis.stc.example.util.PropertyLoader

class KinesisProducer() extends IStreamProducer with PropertyLoader {
  private val kinesisConf = loadProperties("kinesis.properties")

  private val clientConfig = new ClientConfiguration()
  kinesisConf.get("http.proxyHost").filter(_.nonEmpty).map(v=>clientConfig.setProxyHost(v))
  kinesisConf.get("http.proxyPort").filter(_.nonEmpty).map(v=>clientConfig.setProxyPort(v.toInt))
  kinesisConf.get("http.proxyUser").filter(_.nonEmpty).map(v=>clientConfig.setProxyUsername(v))
  kinesisConf.get("http.proxyPassword").filter(_.nonEmpty).map(v=>clientConfig.setProxyPassword(v))

  private val client = new AmazonKinesisClient(new ClasspathPropertiesFileCredentialsProvider(), clientConfig)

  def send(message:String):Unit = {
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(kinesisConf("kinesis.streamName"))
    putRecordRequest.setData(ByteBuffer.wrap(message.getBytes("UTF-8")))
    putRecordRequest.setPartitionKey("key")
    val putRecordResult = client.putRecord(putRecordRequest)
  }
}
