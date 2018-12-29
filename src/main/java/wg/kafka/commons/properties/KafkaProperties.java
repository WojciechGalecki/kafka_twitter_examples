package wg.kafka.commons.properties;

import java.io.IOException;
import java.util.Properties;

public class KafkaProperties {
    private static final String RESOURCE_FILE_NAME = "application.yml";

    private final Properties properties;

    public KafkaProperties() throws IOException {
        this.properties = PropertiesLoader.loadProperties(RESOURCE_FILE_NAME);
    }

    public Properties getProperties() {
        return properties;
    }
}
