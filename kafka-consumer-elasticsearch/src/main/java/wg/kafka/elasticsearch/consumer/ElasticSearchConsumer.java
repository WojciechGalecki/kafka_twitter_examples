package wg.kafka.elasticsearch.consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wg.kafka.commons.properties.KafkaProperties;
import wg.kafka.elasticsearch.client.ElasticSearchClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static final String INDEX = "twitter";
    private static final String TYPE = "tweets";
    private static final String TOPIC_NAME_PROPERTY = "topic";
    private static final long SLEEP_VALUE_MS = 1000;
    private static final long POLL_VALUE_MS = 100;
    private static final String TWEET_ID_JSON_VALUE = "id_str";

    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private ElasticSearchClient elasticSearchClient;

    private Properties properties;

    public ElasticSearchConsumer() throws IOException {
        this.elasticSearchClient = new ElasticSearchClient();
        this.properties = new KafkaProperties().getProperties();
    }

    public static void main(String[] args) throws IOException {
        new ElasticSearchConsumer().run();
    }

    public void run() throws IOException {
        RestHighLevelClient client = elasticSearchClient.getElasticSearchClient();

        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer(properties.getProperty(TOPIC_NAME_PROPERTY));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(POLL_VALUE_MS));

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                try {
                    String tweetId = extractIdFromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, tweetId)
                            .source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: " + record.value());
                }

            }

            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed");

                sleep(SLEEP_VALUE_MS);
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private KafkaConsumer<String, String> getKafkaConsumer(String topicName) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topicName));
        return kafkaConsumer;
    }

    private String extractIdFromTweet(String jsonTweet) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(jsonTweet)
                .getAsJsonObject()
                .get(TWEET_ID_JSON_VALUE)
                .getAsString();
    }
}
