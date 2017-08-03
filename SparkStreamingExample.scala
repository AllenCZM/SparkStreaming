import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by Administrator on 2017/5/10 0010.
  */
object SparkStreamingExample {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //wordCounts.saveAsTextFiles(args(1))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

    /*val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
    //基于一个RDD队列创建一个输入源
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10,1))
    val reduceStream = mappedStream.reduceByKey(_ + _)
    reduceStream.print
    ssc.start()
    for(i <- 1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(1 to 100, 2)   //创建RDD，并分配两个核数
      Thread.sleep(1000)
    }
    ssc.stop()*/
  }
}
