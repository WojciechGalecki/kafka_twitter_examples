package wg.kafka.elasticsearch.consumer;

import org.apache.lucene.index.IndexOptions;
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
import java.util.Properties;

public class ElasticSearchConsumer {
    private static final String INDEX = "twitter";
    private static final String TYPE = "tweets";

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

        String jsonString = "{ \"foo\": \"bar\"}";

        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE)
                .source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        logger.info(indexResponse.getId());

        client.close();
    }
}
