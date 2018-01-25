package lab.zlren.streaming.aiop.log.dao

import lab.zlren.streaming.aiop.log.entity.{HBaseData, PlatformLog}
import lab.zlren.streaming.aiop.log.util.HBaseUtil

import scala.collection.mutable.ListBuffer

/**
  * 保存数据到Hbase
  */
object PlatformLogDAO {

	val tableName = "aiop_log"
	val columnFamilyInfo = "info"
	val columnFamilyDomainNlp = "domain_nlp"
	val columnFamilyDomainAuth = "domain_auth"

	def save(list: ListBuffer[PlatformLog]): Unit = {
		for (elem <- list) {
			save(elem)
		}
	}

	def save(platformLog: PlatformLog): Unit = {

		val dataList = new java.util.ArrayList[HBaseData]()

		// 列族info下包含的列
		dataList.add(HBaseData(columnFamilyInfo, "level", platformLog.level))
		dataList.add(HBaseData(columnFamilyInfo, "time", platformLog.time))
		dataList.add(HBaseData(columnFamilyInfo, "app_id", platformLog.appId))
		dataList.add(HBaseData(columnFamilyInfo, "file", platformLog.file))

		// 列族domain_nlp
		dataList.add(HBaseData(columnFamilyDomainNlp, "ability", platformLog.domainNlpAbility))
		dataList.add(HBaseData(columnFamilyDomainNlp, "action", platformLog.domainNlpAction))
		dataList.add(HBaseData(columnFamilyDomainNlp, "result", platformLog.domainNlpResult))
		dataList.add(HBaseData(columnFamilyDomainNlp, "result_reason", platformLog.domainNlpResultReason))

		// 列族domain_auth
		dataList.add(HBaseData(columnFamilyDomainAuth, "type", platformLog.domainAuthType))
		dataList.add(HBaseData(columnFamilyDomainAuth, "action", platformLog.domainAuthAction))
		dataList.add(HBaseData(columnFamilyDomainAuth, "payload", platformLog.domainAuthPayload))
		dataList.add(HBaseData(columnFamilyDomainAuth, "result", platformLog.domainAuthResult))

		val rowKey = platformLog.level + "_" + platformLog.time + "_" + platformLog.appId
		HBaseUtil.getInstance().putList(tableName, rowKey, dataList)
	}
}
