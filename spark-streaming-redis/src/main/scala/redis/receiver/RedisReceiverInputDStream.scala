package redis.receiver

/**
 * From https://github.com/Anchormen/spark-redis-connector
 */

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver


class RedisReceiverInputDStream(@transient ssc_ : StreamingContext,
                                params: Map[String, String], keySet: Set[String])
  extends ReceiverInputDStream[(String, String)](ssc_) with Logging {

  val _storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2

  override def getReceiver(): Receiver[(String, String)] = {
    params.get("struct").getOrElse("list") match {
      case "list" => new RedisListReceiver(params, keySet, _storageLevel)
      case "set" => new RedisSetReceiver(params, keySet, _storageLevel)
      case _ => throw new IllegalArgumentException("unsupported Redis Structure. The only supported structuers are (1)list and (2)set")
    }
  }
}