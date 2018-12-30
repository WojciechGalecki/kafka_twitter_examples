package wg.kafka.elasticsearch.consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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

            for (ConsumerRecord<String, String> record : records) {
                String tweetId = extractIdFromTweet(record.value());
                IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, tweetId)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());

                try {
                    Thread.sleep(SLEEP_VALUE_MS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
