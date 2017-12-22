package lab.zlren.streaming.project.entity

/**
  * 搜索引擎引流过来的日访问量
  *
  * @param day_search_course hbase中的rowkey 20171111_sogou_131
  * @param click_count             日引流量
  */
case class CourseSearchClickCount(day_search_course: String, click_count: Int)
