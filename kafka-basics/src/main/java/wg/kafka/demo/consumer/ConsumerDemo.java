package wg.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wg.kafka.demo.properties.PropertiesDemo;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {

    private static final String TOPIC = "demo_topic";

    private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {
        new ConsumerDemo().run(TOPIC);
    }

    private ConsumerDemo() {
    }

    private void run(String topic) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerRunnable(topic, countDownLatch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        private CountDownLatch latch;

        private KafkaConsumer<String, String> consumer;

        private PropertiesDemo propertiesDemo = new PropertiesDemo();

        public ConsumerRunnable(String topic, CountDownLatch latch) {
            this.latch = latch;
            consumer = new KafkaConsumer<>(propertiesDemo.getProperties());
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            consumer.wakeup();
        }
    }
}
