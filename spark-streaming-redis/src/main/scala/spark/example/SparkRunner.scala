
package spark.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.receiver._

/**
 * Example program to consume and process streaming data with Spark Streaming
 *
 * See http://spark.apache.org/docs/latest/streaming-programming-guide.html for more info
 *
 * Use "nc -lk 1337" to create local TCP server
 *
 * http://localhost:4040/ to see Spark dashboard
 */

object SparkRunner {

  val tcpPort = 1337
  val batchDurationMilliseconds = new Duration(4 * 1000)
  val generateData = true
  val messageSet = "words"

  def main(args: Array[String]) = {
    println("Setting up the Spark app")

    if (generateData)
      DataSourceProvider.initRedisDataStream(messageSet)

    val ssc: StreamingContext = createStreamingContext()
    val stream: ReceiverInputDStream[(String, String)] = createStream(ssc)

    val streamValues = stream.map(kv => kv._2)

    //MyStreamProcessor.processStream(stream)
    MyStreamProcessor.processStreamCountWordLength(streamValues)
    println("Transformation sequence created")

    ssc.start()
    ssc.awaitTermination()
  }

  def createStream(ssc: StreamingContext): ReceiverInputDStream[(String, String)] = {
    //ssc.socketTextStream("localhost", tcpPort)
    val clusterParams = Map("host" -> "localhost", "port" -> "6379", "cluster" -> "false", "timeout" -> "0", "struct" -> "set")
    val keySet = Set(messageSet)
    new RedisReceiverInputDStream(ssc, clusterParams, keySet)
  }

  def createStreamingContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("streaming-example")
    sparkConf.setMaster("local[*]")
    //sparkConf.set("spark.default.parallelism", Runtime.getRuntime.availableProcessors.toString())
    new StreamingContext(sparkConf, batchDurationMilliseconds)
  }
}