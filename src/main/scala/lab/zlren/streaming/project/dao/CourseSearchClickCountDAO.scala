package lab.zlren.streaming.project.dao

import lab.zlren.streaming.project.entity.CourseSearchClickCount
import lab.zlren.streaming.project.util.HBaseUtilOld
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 日点击量，搜索引擎
  */
object CourseSearchClickCountDAO {

	val tableName = "course_search_clickcount"
	val columnFamily = "info"
	val qualifer = "click_count"

	/**
	  * 累加，保存数据到hbase
	  *
	  * @param list
	  */
	def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

		val table = HBaseUtilOld.getInstance().getTable(tableName)
		for (elem <- list) {
			// 增量更新
			table.incrementColumnValue(
				elem.day_search_course.getBytes(),
				columnFamily.getBytes(),
				qualifer.getBytes(),
				elem.click_count)
		}
	}

	/**
	  * 根据rowkey查询
	  *
	  * @param day_course
	  * @return
	  */
	def count(day_course: String): Long = {

		val table = HBaseUtilOld.getInstance().getTable(tableName)
		val get = new Get(day_course.getBytes())
		val value = table.get(get).getValue(columnFamily.getBytes(), qualifer.getBytes())

		if (value == null) {
			0L
		} else {
			Bytes.toLong(value)
		}
	}

	def main(args: Array[String]): Unit = {
		val list = new ListBuffer[CourseSearchClickCount]
		list.append(CourseSearchClickCount("20171111_baidu_11", 11))
		list.append(CourseSearchClickCount("20171111_sogou_12", 12))
		list.append(CourseSearchClickCount("20171111_bing_13", 13))
		save(list)

		println(count("20171111_baidu_11"))
		println(count("20171111_sogou_12"))
		println(count("20171111_bing_13"))
	}
}
