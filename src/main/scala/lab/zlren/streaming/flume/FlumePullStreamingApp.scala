package lab.zlren.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Streaming主动从Flume拉取数据
  */
object FlumePullStreamingApp {

	def main(args: Array[String]): Unit = {

		if (args.length != 2) {
			System.err.println("<hostname> <port>")
			System.exit(1)
		}

		val Array(hostname, port) = args

		val sparkConf = new SparkConf().setAppName("FlumePullStreamingApp").setMaster("local[*]")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		// 这里是关键
		val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)
		val result = flumeStream.map(x => new String(x.event.getBody.array()).trim).flatMap(_.split(" ")).map((_, 1))
		  .reduceByKey(_ + _)
		result.print()

		ssc.start()
		ssc.awaitTermination()
	}
}
