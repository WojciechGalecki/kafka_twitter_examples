package wg.kafka.twitter.properties;

import java.io.IOException;
import java.util.Properties;

public class TwitterProperties {

    private static final String CONSUMER_KEY = "CONSUMER_KEY";
    private static final String CONSUMER_SECRET = "CONSUMER_SECRET";
    private static final String TOKEN = "TOKEN";
    private static final String SECRET = "SECRET";

    private Properties properties;

    /**secrets from twitter developer account*/
    private String consumer_key;
    private String consumer_secret;
    private String token;
    private String secret;

    public TwitterProperties(String resourceFileName) throws IOException {
        this.properties = PropertiesLoader.loadProperties(resourceFileName);
        this.consumer_key = getProperty(CONSUMER_KEY);
        this.consumer_secret = getProperty(CONSUMER_SECRET);
        this.token = getProperty(TOKEN);
        this.secret = getProperty(SECRET);
    }

    public String getConsumer_key() {
        return consumer_key;
    }

    public String getConsumer_secret() {
        return consumer_secret;
    }

    public String getToken() {
        return token;
    }

    public String getSecret() {
        return secret;
    }

    private String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }
}
