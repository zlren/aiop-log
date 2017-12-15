package lab.zlren.streaming.kafka;

import lab.zlren.streaming.kafka.handler.AbstractMsgHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Collections;
import java.util.Properties;

/**
 * @author zlren
 * @date 2017-12-09
 */
public class MessageConsumer extends Thread {

    private String topic;

    Consumer<Integer, String> consumer;

    AbstractMsgHandler msgHandler;

    public MessageConsumer(String topic, AbstractMsgHandler msgHandler) {

        this.topic = topic;
        this.msgHandler = msgHandler;

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);

        // 消费者组
        props.put("group.id", "my_group");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100); //每100秒poll一次
            for (ConsumerRecord<Integer, String> record : records) {
                msgHandler.onMessage(record);
            }
        }
    }
}
