package lab.zlren.streaming.project.entity

/**
  *
  * @param ip         ip地址
  * @param time       时间戳
  * @param courseId   课程编号
  * @param statusCode 状态码
  * @param referer    refer信息
  */
case class ClickLog(ip: String, time: String, courseId: Int, statusCode: Int, referer: String) {

}
