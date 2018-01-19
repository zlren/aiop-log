package lab.zlren.streaming.project

import lab.zlren.streaming.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import lab.zlren.streaming.project.entity.{ClickLog, CourseClickCount, CourseSearchClickCount}
import lab.zlren.streaming.project.util.DateUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object ProjectApp {

	val logger: Logger = LoggerFactory.getLogger(ProjectApp.getClass)

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("DirectApproachFromKafka").setMaster("local[*]")
		val ssc = new StreamingContext(sparkConf, Seconds(10))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "10.109.246.66:9092",
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

		try {
			// 放在try里还是稳一些放置宕机，

			val cleanedData = logs.filter(log => {
				// 简单过滤一下，放置有坏的数据
				log.split("\t").length == 5
			}).map(log => {
				// 143.63.124.156	2017-12-21 22:23:01	"GET class/131.html HTTP/1.1"	404	http://www.sogou.com/web?query=spark-streaming实战

				val infos = log.split("\t")
				logger.info("本条数据是：" + log)
				val url = infos(2).split(" ")(1) // class/145.html
				var courseId = 0 // 默认课程编号为0

				// 只分析实战课程 class开头的
				if (url.startsWith("class")) {
					val courseHtml = url.split("/")(1) // 145.html
					courseId = courseHtml.split("\\.")(0).toInt
				}

				ClickLog(infos(0), DateUtil.getTime(infos(1)), courseId, infos(3).toInt, infos(4))
			}).filter(clickLog => clickLog.courseId != 0) // 过滤掉课程号为0的才是实战课程

			// 日访问量
			cleanedData.map(x => {
				// hbase的rowkey设计
				(x.time.substring(0, 8) + "_" + x.courseId, 1)
			}).reduceByKey(_ + _).foreachRDD(rdd => {
				rdd.foreachPartition(partitionRecords => {
					val list = new ListBuffer[CourseClickCount]
					partitionRecords.foreach(pair => {
						list.append(CourseClickCount(pair._1, pair._2))
					})
					CourseClickCountDAO.save(list)
				})
			})

			// 搜索引擎日引流量
			cleanedData.filter(clickLog => clickLog.referer != "-").map(clickLog => {
				(clickLog.time.substring(0, 8) + "_" + clickLog.referer.split("\\.")(1) + "_" + clickLog.courseId, 1)
			}).reduceByKey(_ + _).foreachRDD(rdd => {
				rdd.foreachPartition(partitionRecords => {
					val list = new ListBuffer[CourseSearchClickCount]
					partitionRecords.foreach(pair => {
						list.append(CourseSearchClickCount(pair._1, pair._2))
					})
					CourseSearchClickCountDAO.save(list)
				})
			})
		} catch {
			case e: Exception => {
				logger.error(e.getMessage)
			}
		}

		ssc.start()
		ssc.awaitTermination()
	}
}
