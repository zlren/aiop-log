package lab.zlren.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Flume源端采集数据完成后主动推送到Streaming程序
  */
object FlumePushStreamingApp {

	def main(args: Array[String]): Unit = {

		if (args.length != 2) {
			System.err.println("<hostname> <port>")
			System.exit(1)
		}

		val Array(hostname, port) = args

		val sparkConf = new SparkConf().setAppName("FlumePushStreamingApp").setMaster("local[*]")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)
		val result = flumeStream.map(x => new String(x.event.getBody.array()).trim).flatMap(_.split(" ")).map((_, 1))
		  .reduceByKey(_ + _)
		result.print()


		ssc.start()
		ssc.awaitTermination()
	}
}
