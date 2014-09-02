package jp.co.tis.stc.example

import twitter4j._
import twitter4j.conf._

import jp.co.tis.stc.example.kafka.producer._

object TweetStreamer {
  def main(args:Array[String]) {
    //val producer = new TweetStreamProducer(args(0), args(1))
    val producer = StreamProducerFactory.getInstance(args(0))
    val twitterWrapper = new TwitterWrapper(tweet => {
      println(tweet)
      producer.send(tweet)
    })
    twitterWrapper.start(Array(args(1)))
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
      conf.get("http.proxyHost").map(k=>cb.setHttpProxyHost(k))
      conf.get("http.proxyPort").map(k=>cb.setHttpProxyPort(k.toInt))
      conf.get("http.proxyUser").map(k=>cb.setHttpProxyUser(k))
      conf.get("http.proxyPassword").map(k=>cb.setHttpProxyPassword(k))
      
      val twitterStream = new TwitterStreamFactory(cb.build).getInstance
      twitterStream.addListener(listener)
      twitterStream
    }
  }
}

