package jp.co.tis.stc.example

import twitter4j._
import twitter4j.conf._

import jp.co.tis.stc.example.util.PropertyLoader

object TweetStreamer extends LogHelper with PropertyLoader {
  def main(args:Array[String]) {
    if (args.length < 2) {
      println("Usage: java -jar tweetstreamer.jar (TEST|KAFKA|KINESIS) query_words...")
      sys.exit(1)
    }
    val producer = StreamProducerFactory.getInstance(args.head)
    val twitterWrapper = new TwitterWrapper(tweet => {
      logger.info(tweet)
      producer.send(tweet)
    })
    logger.info("QueryWords => %s".format(args.tail.mkString(" "))) 
    twitterWrapper.start(args.tail)
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

      val twitterConf = loadProperties("twitter.properties")
      
      val cb = new ConfigurationBuilder()
      cb.setOAuthConsumerKey(twitterConf("oauth.consumerKey"))
      cb.setOAuthConsumerSecret(twitterConf("oauth.consumerSecret"))
      cb.setOAuthAccessToken(twitterConf("oauth.accessToken"))
      cb.setOAuthAccessTokenSecret(twitterConf("oauth.accessTokenSecret"))
      twitterConf.get("http.proxyHost").filter(_.nonEmpty).map(v=>cb.setHttpProxyHost(v))
      twitterConf.get("http.proxyPort").filter(_.nonEmpty).map(v=>cb.setHttpProxyPort(v.toInt))
      twitterConf.get("http.proxyUser").filter(_.nonEmpty).map(v=>cb.setHttpProxyUser(v))
      twitterConf.get("http.proxyPassword").filter(_.nonEmpty).map(v=>cb.setHttpProxyPassword(v))
      
      val twitterStream = new TwitterStreamFactory(cb.build).getInstance
      twitterStream.addListener(listener)
      twitterStream
    }
  }
}

trait LogHelper {
  lazy val logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)
}
