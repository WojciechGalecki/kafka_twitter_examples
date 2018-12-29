package wg.kafka.twitter.producer;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wg.kafka.twitter.client.TwitterClient;
import wg.kafka.commons.properties.KafkaProperties;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static final String TOPIC_NAME = "topic";
    private static final int BLOCKING_QUEUE_CAPACITY = 100000;
    private static final long TIMEOUT_S = 5;

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private TwitterClient twitterClient;

    private Properties properties;

    public TwitterProducer() throws IOException {
        this.twitterClient = new TwitterClient();
        this.properties = new KafkaProperties().getProperties();
    }

    public static void main(String[] args) throws IOException {
        new TwitterProducer().run();
    }

    public void run() throws IOException {
        logger.info("Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);

        Client client = twitterClient.getTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        addShutdownHook(client, kafkaProducer);

        while (!client.isDone()) {
            String twitterMessage = null;
            try {
                twitterMessage = msgQueue.poll(TIMEOUT_S, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (twitterMessage != null) {
                kafkaProducer.send(new ProducerRecord<>(properties.getProperty(TOPIC_NAME), null, twitterMessage),
                        this::onCompletion);
            }
        }
        logger.info("End off application");
    }

    private void addShutdownHook(Client client, KafkaProducer<String, String> kafkaProducer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            client.stop();
            kafkaProducer.close();
            logger.info("Application was closed!");
        }
        ));
    }

    private void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            logger.error("Error during sending record into kafka: ", e);
        } else {
            logger.info("Sending twitter message, timestamp: " + String.valueOf(recordMetadata.timestamp()));
        }
    }
}