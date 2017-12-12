package lab.zlren.streaming.usecase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用SparkStreaming完成有状态的统计
  */
object StatefulWordCount {

	def main(args: Array[String]): Unit = {

		val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(10))
		// 如果使用了状态算子，必须要设置checkpoint目录
		ssc.checkpoint("/tmp/checkpoint")

		val lines = ssc.socketTextStream("localhost", 6789)
		val result = lines.flatMap(_.split(" ")).map((_, 1))
		val state = result.updateStateByKey(updateFunction)

		// print是将结果打印
		state.print()

		ssc.start()
		ssc.awaitTermination()
	}

	/**
	  * 把当前的数据去更新已有的或者是老的数据
	  *
	  * @param curValues
	  * @param preValues
	  * @return
	  */
	def updateFunction(curValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
		val curCount = curValues.sum
		val preCount = preValues.getOrElse(0)
		Some(curCount + preCount)
	}
}
