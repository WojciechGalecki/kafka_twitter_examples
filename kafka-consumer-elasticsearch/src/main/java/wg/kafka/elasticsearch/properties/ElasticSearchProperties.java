package wg.kafka.elasticsearch.properties;

import wg.kafka.commons.properties.PropertiesLoader;

import java.io.IOException;
import java.util.Properties;

public class ElasticSearchProperties {

    private static final String HOSTNAME = "HOSTNAME";
    private static final String USERNAME = "USERNAME";
    private static final String PASSWORD = "PASSWORD";

    private Properties properties;

    /**secrets from https://app.bonsai.io elasticsearch free web cluster*/
    private String hostname;
    private String username;
    private String password;

    public ElasticSearchProperties(String resourceFileName) throws IOException {
        this.properties = PropertiesLoader.loadProperties(resourceFileName);
        this.hostname = getProperty(HOSTNAME);
        this.username = getProperty(USERNAME);
        this.password = getProperty(PASSWORD);
    }

    public String getHostname() {
        return hostname;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    private String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }
}
