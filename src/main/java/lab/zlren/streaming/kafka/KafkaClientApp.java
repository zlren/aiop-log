package lab.zlren.streaming.kafka;

import lab.zlren.streaming.kafka.handler.HelloMsgHandler;

import java.util.Scanner;

/**
 * 客户端测试
 * @author zlren
 * @date 2017-12-09
 */
public class KafkaClientApp {

    public static void main(String[] args) throws InterruptedException {

        // 创建TOPIC_HELLO的消息消费者
        new MessageConsumer(KafkaProperties.TOPIC_HELLO, new HelloMsgHandler()).start();
        Thread.sleep(1000);

        // 创建通用消息生产者
        MessageProducer messageProducer = new MessageProducer();


        //等待输入，模拟生产消息
        Scanner scanner = new Scanner(System.in);
        int msgNumber = 1;
        while (true) {
            String msgContent = scanner.nextLine();
            messageProducer.send(KafkaProperties.TOPIC_HELLO, msgNumber++, msgContent);
        }
    }
}
