package lab.zlren.streaming.kafka;

/**
 * @author zlren
 * @date 2017-12-09
 */
public class KafkaClientApp {

    public static void main(String[] args) throws InterruptedException {
        new MyConsumer(KafkaProperties.TOPIC).start();
        Thread.sleep(1000);
        new MyProducer(KafkaProperties.TOPIC).start();
    }
}
