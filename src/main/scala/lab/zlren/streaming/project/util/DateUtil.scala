package lab.zlren.streaming.project.util

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间工具类
  */
object DateUtil {

	val originFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
	val targetFormat: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

	def getTime(time: String): String = {
		targetFormat.format(originFormat.parse(time).getTime)
	}

	def main(args: Array[String]): Unit = {
		println(getTime("2017-12-21 21:18:01"))
	}
}
