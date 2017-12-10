package lab.zlren.streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author zlren
 * @date 2017-12-09
 */
public class MyConsumer extends Thread {

    private String topic;

    KafkaConsumer<Integer, String> consumer;

    public MyConsumer(String topic) {

        this.topic = topic;

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);

        // 消费者组
        props.put("group.id", "my_group");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        while (true) {
            // 100是超时等待时间
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
            }
        }
    }
}
