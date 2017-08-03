import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Administrator on 2017/5/11 0011.
  */
object KafkaStreamingExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")  //.setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))
   // ssc.checkpoint("D:\\scala-file\\checkpoint")
    ssc.checkpoint("/opt/spark-1.6.3-bin-hadoop2.6/test/checkpoint")
    val topics = List(("pandas", 1), ("logs", 1)).toMap
    val zkQuorum = "master:2181"
    val group = "test-consumer-group"

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topics).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
