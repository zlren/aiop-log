package lab.zlren.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * kafka_0.10后只支持direct模式（no receivers）
  */
object DirectApproachFromKafka {

	def main(args: Array[String]): Unit = {

		val sparkConf = new SparkConf().setAppName("DirectApproachFromKafka").setMaster("local[*]")
		val ssc = new StreamingContext(sparkConf, Seconds(5))

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "data:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "use_a_separate_group_id_for_each_stream",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val topics = Array("hello_topic")

		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](topics, kafkaParams)
		)

		// _.key()是null，_.value()对我们是有用的
		stream.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

		ssc.start()
		ssc.awaitTermination()
	}
}
