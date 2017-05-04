package spark.example

import com.google.gson.Gson
import org.apache.spark.streaming.dstream.DStream
import java.util.Date

/**
 * Created by brobrien on 10/26/16.
 *
 * Redis streams produced:
 *
 * stream-transitions
 * summary-metrics
 * detail-metrics
 * raw-messages
 *
 */
object MyStreamProcessor {

  val gson = new Gson()

  val sourceName = "Source Stream"
  val longWordsName = "Long Words"
  val shortWordsName = "Short Words"

  val longWordsPosName = "Positive Sentiment"
  val longWordsNegName = "Negative Sentiment"

  val persistedWordsName = "Persisted in Database"


  //classify word based on first letter
  def isPositiveSentiment(word: String): Boolean = {
    val firstLetterASCII = word.charAt(0)
    firstLetterASCII.toInt > 110
  }

  def isLongWord(word: String): Boolean = {
    word.length > 3
  }

  def processStreamCountWordLength(stream: DStream[String]): Unit = {

    //Create complex DAG for illustration purposes
    stream.foreachRDD(wordList => {
      wordList.persist() //persist RDD to avoid unnecessary recomputation

      val longWords = wordList.filter(isLongWord(_))
      val shortWords = wordList.filter(!isLongWord(_))

      val longPosWords = longWords.filter(isPositiveSentiment(_))
      val longNegWords = longWords.filter(!isPositiveSentiment(_))

      //val shortPosWords = shortWords.filter(isPositiveSentiment(_))
      //val shortNegWords = shortWords.filter(!isPositiveSentiment(_))

      //Build logic representation of stream transition graph for later visualization
      val s1 = StreamTransition(sourceName, longWordsName, longWords.count())
      val s2 = StreamTransition(sourceName, shortWordsName, shortWords.count())
      val s3 = StreamTransition(longWordsName, longWordsPosName, longPosWords.count())
      val s4 = StreamTransition(longWordsName, longWordsNegName, longNegWords.count())
      //val s5 = StreamTransition(shortWordsName, shortWordsPosName, shortPosWords.count())
      //val s6 = StreamTransition(shortWordsName, shortWordsNegName, shortNegWords.count())
      val s7 = StreamTransition(shortWordsName, persistedWordsName, shortWords.count())
      val s8 = StreamTransition(longWordsPosName, persistedWordsName, longPosWords.count())
      val s9 = StreamTransition(longWordsNegName, persistedWordsName, longNegWords.count())

      val transitions = Array(s1, s2, s3, s4, s7, s8, s9)
      val streamTransitionsJson = gson.toJson(transitions)
      JedisProvider.exec(_.publish("stream-transitions", streamTransitionsJson))
    })

    //Compute Summary Metrics
    stream.foreachRDD(wordList => {
      val batchOfWords = wordList.collect() //normally you wouldn't do this in a production scenario
      val totalWords = batchOfWords.length
      val totalCharacters = batchOfWords.foldLeft(0) { (sum, word) => sum + word.length }
      val avgCharsPerWord = if (totalWords == 0) 0 else (totalCharacters.toDouble / totalWords)

      val summaryMetrics = SummaryMetrics(totalWords, totalCharacters, avgCharsPerWord, new Date())

      val jsonSummaryData = gson.toJson(summaryMetrics)
      println(jsonSummaryData)
      JedisProvider.exec(_.publish("summary-metrics", jsonSummaryData))

      JedisProvider.exec(jedis => {
        batchOfWords.foreach(word => {
          jedis.publish("raw-messages", word)
        })
      })
    })

    //Compute Detail Metrics
    val wordLengths: DStream[(Int, Int)] = stream.map(word => (word.length, 1))
    val countsByLength: DStream[(Int, Int)] = wordLengths.reduceByKey((w1, w2) => w1 + w2)

    //dstream.foreachRDD triggers actual execution
    countsByLength.foreachRDD(rdd => {
      println("new RDD")

      val countsCollection = for (wordLengthCount <- rdd.collect()) yield {
        val length = wordLengthCount._1
        val count = wordLengthCount._2
        println(s"$count words with length $length observed")
        DetailCount(length, count)
      }
      val jsonDetailData = gson.toJson(countsCollection)
      JedisProvider.exec(_.publish("detail-metrics", jsonDetailData))
    })
  }

  def processStream(stream: DStream[String]): Unit = {
    //DStream is just a sequence of RDD's
    stream.foreachRDD(rdd => {
      //This function executed on the driver

      rdd.foreachPartition(partition => {
        //This function executed on the executor(s)

        //RDD partitions are processed in parallel, but elements in a single partition are processed serially
        partition.foreach(message => {
          println("Received message: " + message)
          JedisProvider.exec(jedis => {
            jedis.publish("raw-messages", message)
          })
        })
      })
    })
  }
}

case class SummaryMetrics(totalWords: Int, totalCharacters: Int, avgCharsPerWord: Double, lastRDDTime: Date)

case class DetailCount(wordLength: Int, count: Int)

case class StreamTransition(from: String, to: String, count: Long)