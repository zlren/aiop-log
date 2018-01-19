package lab.zlren.streaming.aiop.log.entity

/**
  *
  * @param level                 日志等级
  * @param time                  时间戳
  * @param file                  日志文件
  * @param appId                 app id
  * @param domainNlpAbility      能力名称
  * @param domainNlpAction       动作
  * @param domainNlpResult       结果
  * @param domainNlpResultReason 失败原因
  * @param domainAuthAction      鉴权动作
  * @param domainAuthType        鉴权Type，access_token，permission
  * @param domainAuthPayload     鉴权数据
  * @param domainAuthResult      鉴权结果
  */
case class PlatformLog(level: String,
					   time: String,
					   file: String,
					   appId: String,
					   domainNlpAbility: String,
					   domainNlpAction: String,
					   domainNlpResult: String,
					   domainNlpResultReason: String,
					   domainAuthAction: String,
					   domainAuthType: String,
					   domainAuthPayload: String,
					   domainAuthResult: String) {

}

/**
  * 表名：aiop-log
  * rowKey: level + time + app_id
  *- info
  *- 	level
  *- 	time
  *- 	file
  *- domain_nlp
  *- 	app_id
  *- 	ability
  *- 	action(CACHE_HIT、INVOKE)
  *- 	result_code
  *- 	result_reason
  *- domain_auth
  *- 	action（AUTH)
  *- 	app_id
  *- 	type（access_token、permission）
  *- 	payload
  *- 	result
  */
