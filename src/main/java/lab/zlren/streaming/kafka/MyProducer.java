package lab.zlren.streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka生产者
 *
 * @author zlren
 * @date 2017-12-09
 */
public class MyProducer extends Thread {

    private String topic;

    private Producer<Integer, String> producer;

    public MyProducer(String topic) {

        this.topic = topic;

        Properties props = new Properties();

        // bootstrap.servers是新版的api
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);

        // ack方式，all，会等所有的commit，最慢的方式
        props.put("acks", "1");

        // // 失败是否重试，设置会有可能产生重复数据
        // props.put("retries", 0);
        //
        // // 对于每个partition的batch buffer大小
        // props.put("batch.size", 16384);
        //
        // // 等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        // props.put("linger.ms", 1);
        //
        // // 整个producer可以用于buffer的内存大小
        // props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        int msgNumber = 1;

        while (true) {

            String msg = "msg_" + msgNumber;
            producer.send(new ProducerRecord<>(topic, msgNumber++, msg));
            System.out.println("sent: " + msg);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
