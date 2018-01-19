package lab.zlren.streaming.aiop.log

object Test {

	def main(args: Array[String]): Unit = {
		println("INFO  2018-01-18 21:15:17 (LogAspect.java:65) - domain=NLP_REST: app_id=1 ENTER ability=word_pos".startsWith
		("INFO"))
	}
}
