package jp.co.tis.stc.example

import twitter4j._
import twitter4j.conf._

import jp.co.tis.stc.example.kafka.producer._

object TweetStreamer extends LogHelper {
  def main(args:Array[String]) {
    val producer = StreamProducerFactory.getInstance(args(0))
    val twitterWrapper = new TwitterWrapper(tweet => {
      logger.info(tweet)
      producer.send(tweet)
    })
    twitterWrapper.start(Array("storm", "kafka", "kinesis"))
  }

  private class TwitterWrapper(func:(String) => Unit) {
    import twitter4j._
    import twitter4j.conf._
    import scala.collection.JavaConverters._

    private val twitterStream = init

    def start(filters:Array[String]) {
      twitterStream.filter((new FilterQuery).track(filters))
    }

    private def init = {
      val listener = new StatusListener {
        def onDeletionNotice(sdn:StatusDeletionNotice) = println("onDeletionNotice")
        def onScrubGeo(userId:Long, upToStatusId:Long) = println("onScrubGeo")
        def onStallWarning(warning:StallWarning) = println("onStallWarning")
        def onTrackLimitationNotice(num:Int) = println("onTrackLimitationNotice")
        def onException(ex:java.lang.Exception) = println("onException:%s".format(ex.toString))
        def onStatus(status:Status) = {
          func(status.getText)
        }
      }

      val prop = new java.util.Properties()
      prop.load(this.getClass.getClassLoader.getResourceAsStream("twitter.properties"))
      val conf = prop.asScala
      
      val cb = new ConfigurationBuilder()
      cb.setOAuthConsumerKey(conf("oauth.consumerKey"))
      cb.setOAuthConsumerSecret(conf("oauth.consumerSecret"))
      cb.setOAuthAccessToken(conf("oauth.accessToken"))
      cb.setOAuthAccessTokenSecret(conf("oauth.accessTokenSecret"))
      conf.get("http.proxyHost").filter(_.nonEmpty).map(v=>cb.setHttpProxyHost(v))
      conf.get("http.proxyPort").filter(_.nonEmpty).map(v=>cb.setHttpProxyPort(v.toInt))
      conf.get("http.proxyUser").filter(_.nonEmpty).map(v=>cb.setHttpProxyUser(v))
      conf.get("http.proxyPassword").filter(_.nonEmpty).map(v=>cb.setHttpProxyPassword(v))
      
      val twitterStream = new TwitterStreamFactory(cb.build).getInstance
      twitterStream.addListener(listener)
      twitterStream
    }
  }
}

trait LogHelper {
  lazy val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)
}
