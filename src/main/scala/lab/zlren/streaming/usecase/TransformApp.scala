package lab.zlren.streaming.usecase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {
	def main(args: Array[String]): Unit = {

		val sparkConf = new SparkConf().setAppName("BlackList").setMaster("local[*]")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		// 如果使用了状态算子，必须要设置checkpoint目录
		// ssc.checkpoint("/tmp/checkpoint")

		// 构建黑名单
		val blackList = List("zs", "ls")
		val blackListRDD = ssc.sparkContext.parallelize(blackList).map((_, false))

		val lines = ssc.socketTextStream("localhost", 6789)
		val clickLogRDD = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
			rdd.leftOuterJoin(blackListRDD).filter(x => {
				x._2._2.getOrElse(true)
			}).map(x => {
				x._2._1
			})
		})

		clickLogRDD.print()

		ssc.start()
		ssc.awaitTermination()
	}
}
