package lab.zlren.streaming.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * hello topic消费行为实现
 */
public class HelloMsgHandler implements AbstractMsgHandler {

	@Override
	public void onMessage(ConsumerRecord<Integer, String> record) {
		System.out.println("consumer handle: offset = " + record.offset() + ", msgNumber = " + record.key() + ", msgContent = " + record.value());
	}
}
