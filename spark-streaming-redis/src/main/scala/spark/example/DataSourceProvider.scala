package spark.example

/**
 * Created by brobrien on 10/25/16.
 */
object DataSourceProvider {

  //Read the file into memory and convert into an array of words, with punctuation removed and all-lowercase
  val stream = getClass().getResourceAsStream("/hitchhikers.txt")
  val corpus = scala.io.Source.fromInputStream(stream).mkString
  val corpusNoPunctuation = corpus.replaceAll("[^A-Za-z0-9\\s]", "")
  val words = corpusNoPunctuation.split("\\s+").map(_.toLowerCase)

  def main(args: Array[String]): Unit = {
    initRedisDataStream("words")
  }

  def initRedisDataStream(messageSet: String) = {
    println("Initializing Redis Data Stream Producer")

    new Thread(new Runnable {
      def run(): Unit = {
        var i = 0
        val r = scala.util.Random
        while (true) {
          val word = words(i % words.length)
          JedisProvider.exec(jedis => {
            jedis.sadd(messageSet, word)
          })
          i += 1
          try {
            Thread.sleep(r.nextInt(300))
          } catch {
            case e: InterruptedException => {}
          }
        }
      }
    }).start
  }

}
