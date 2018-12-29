package wg.kafka.elasticsearch.client;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import wg.kafka.elasticsearch.properties.ElasticSearchProperties;

import java.io.IOException;

public class ElasticSearchClient {
    private static final String RESOURCE_FILE_NAME = "elasticsearch-secrets.yml";
    private static final int HOST_PORT = 443;
    private static final String HOST_SCHEME = "https";

    private ElasticSearchProperties elasticSearchProperties;

    public ElasticSearchClient() throws IOException {
        this.elasticSearchProperties = new ElasticSearchProperties(RESOURCE_FILE_NAME);
    }

    public RestHighLevelClient getElasticSearchClient() {

        final CredentialsProvider credentialsProvider = getCredentialsProvider(elasticSearchProperties.getUsername(),
                elasticSearchProperties.getPassword());

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(elasticSearchProperties.getHostname(), HOST_PORT, HOST_SCHEME)
        ).setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }

    private CredentialsProvider getCredentialsProvider(String userName, String password) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));
        return credentialsProvider;
    }
}
