package wg.kafka.twitter.client;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import wg.kafka.twitter.properties.TwitterProperties;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {
    private static final String RESOURCE_FILE_NAME = "twitter-secrets.yml";
    private static final String TWITTER_MSG_KEY_WORD = "kafka";

    private TwitterProperties twitterProperties;

    public TwitterClient() throws IOException {
        this.twitterProperties = new TwitterProperties(RESOURCE_FILE_NAME);
    }

    public Client getTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts twitterHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint twitterEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList(TWITTER_MSG_KEY_WORD);
        twitterEndpoint.trackTerms(terms);

        Authentication twitterAuth = new OAuth1(twitterProperties.getConsumer_key(), twitterProperties.getConsumer_secret(),
                twitterProperties.getToken(), twitterProperties.getSecret());

        ClientBuilder builder = new ClientBuilder()
                .name("My-Twitter-Client-01")
                .hosts(twitterHosts)
                .authentication(twitterAuth)
                .endpoint(twitterEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
