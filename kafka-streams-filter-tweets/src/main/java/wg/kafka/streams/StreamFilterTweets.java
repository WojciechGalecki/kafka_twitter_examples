package wg.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wg.kafka.commons.properties.KafkaProperties;

import java.io.IOException;
import java.util.Properties;

public class StreamFilterTweets {
    private static final String INPUT_TOPIC_NAME_PROPERTY = "topic-from";
    private static final String OUTPUT_TOPIC_NAME_PROPERTY = "topic-to";
    private static Integer FOLLOWERS_NUMBER = 10000;

    private Properties properties;

    Logger logger = LoggerFactory.getLogger(StreamFilterTweets.class.getName());

    public StreamFilterTweets() throws IOException {
        this.properties = new KafkaProperties().getProperties();
    }

    public static void main(String[] args) throws IOException {
        new StreamFilterTweets().run();
    }

    public void run() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(properties.getProperty(INPUT_TOPIC_NAME_PROPERTY));

        logger.info("Filtering tweets started...");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > FOLLOWERS_NUMBER);

        filteredStream.to(properties.getProperty(OUTPUT_TOPIC_NAME_PROPERTY));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }

    private static Integer extractUserFollowersInTweet(String tweetJson) {
        JsonParser jsonParser = new JsonParser();

        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
