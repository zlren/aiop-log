package lab.zlren.streaming.aiop.log.entity

/**
  * HBase单元数据
  *
  * @param columnFamily 列族
  * @param column       列
  * @param value        值
  */
case class HBaseData(columnFamily: String, column: String, value: String) {

}
