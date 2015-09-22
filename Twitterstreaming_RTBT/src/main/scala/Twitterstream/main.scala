package Twitterstream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Bannu on 21-09-2015.
 */
object main {

  val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")
  //Create a Streaming COntext with 2 second window
  val ssc = new StreamingContext(sparkConf, Seconds(2))


  def main(args: Array[String]) {


    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "wVfr9tcdUP9xB8kZ8bNLFmrh0")
    System.setProperty("twitter4j.oauth.consumerSecret", "40WZ1KAFT7gh5dVKY4UTlRVNoCmlivYz9yxzTsXJpYkW4Bo7LN")
    System.setProperty("twitter4j.oauth.accessToken", "136941445-cZNBS8mCWYGlCXBuf2JRbBxXPOH1hnb2gD58cf9W")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "Y4yyIAUwSo3L01jQeC0Q05ANdl1x0rtOsfWVMxjRKoshF")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls

    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    stream.print()
    //Map : Retrieving Hash Tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Finding the top hash Tags on 30 second window
    val topCounts30 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))
    //Finding the top hash Tgas on 10 second window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts30.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 30 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      SocketClient.sendCommandToRobot("twitter tweets count is "+rdd.count())

      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
    ssc.start()

    ssc.awaitTermination()
  }
}

