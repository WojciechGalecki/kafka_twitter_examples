package wg.kafka.twitter.producer;

import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wg.kafka.twitter.client.TwitterClient;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private TwitterClient twitterClient;

    public TwitterProducer() throws IOException {
        this.twitterClient = new TwitterClient();
    }

    public static void main(String[] args) throws IOException {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client client = twitterClient.getTwitterClient(msgQueue);
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
            }
        }
        logger.info("End off application");
    }
}
