package lab.zlren.streaming.project

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object ProjectApp {

	val logger: Logger = LoggerFactory.getLogger(ProjectApp.getClass)

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("DirectApproachFromKafka").setMaster("local[*]")
		val ssc = new StreamingContext(sparkConf, Seconds(10))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "data:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "use_a_separate_group_id_for_each_stream",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val topics = Array("aiop-log")

		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](topics, kafkaParams)
		)

		// _.key()是null，_.value()对我们是有用的，logs是这一批数据
		val logs = stream.map(_.value())

		val cleanedData = logs.map(log => {

			// 143.63.124.156	2017-12-21 22:23:01	"GET class/131.html HTTP/1.1"	404	http://www.sogou.com/web?query=spark-streaming实战

			val infos = log.split("\t")
			val url = infos(2).split(" ")(1) // class/145.html
			var courseId = 0 // 默认课程编号为0

			// 只分析实战课程 class开头的
			if (url.startsWith("class")) {
				val courseHtml = url.split("/")(1) // 145.html
				courseId = courseHtml.split("\\.")(0).toInt
			}

			ClickLog(infos(0), DateUtils.getTime(infos(1)), courseId, infos(3).toInt, infos(4))
		}).filter(clickLog => clickLog.courseId != 0)

		// 打印清洗后的数据
		cleanedData.print()

		ssc.start()
		ssc.awaitTermination()
	}
}
