package lab.zlren.streaming.usecase

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用SparkStreaming完成词频统计，写入到MySQL
  */
object StatefulWordCountSaveToMysql {

	def main(args: Array[String]): Unit = {

		val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
		val ssc = new StreamingContext(sparkConf, Seconds(10))
		// 如果使用了状态算子，必须要设置checkpoint目录
		ssc.checkpoint("/tmp/checkpoint")

		val lines = ssc.socketTextStream("localhost", 6789)
		val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

		result.foreachRDD(rdd => {
			rdd.foreachPartition(partition => {
				val connection = createConnection()
				partition.foreach(record => {
					// 这里就只是每次插入，更新的操作没有做
					// 还有一个改进就是把数据库的连接使用连接池进行管理
					val sql = "insert into wordcount(word, count) values (" + record._1 + ", " + record._2 + ")"
					connection.createStatement().execute(sql)
				})
				connection.close()
			})
		})

		ssc.start()
		ssc.awaitTermination()
	}

	def createConnection(): Connection = {
		Class.forName("com.mysql.jdbc.Driver")
		DriverManager.getConnection(
			"jdbc:mysql://10.109.246.35:3306/streaming?useUnicode=true&characterEncoding=utf-8&useSSL=false",
			"root",
			"Lab2016!")
	}
}
