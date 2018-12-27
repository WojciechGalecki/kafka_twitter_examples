package wg.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wg.kafka.properties.PropertiesDemo;

public class ProducerDemo {

    public static void main(String[] args) {

        PropertiesDemo propertiesDemo = new PropertiesDemo();

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propertiesDemo.getProperties());

        for (int i = 0; i < 10; i++) {

            String topic = "demo_topic";
            String key = "id_" + Integer.toString(i);
            String value = "hello world_" + Integer.toString(i);


            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        producer.flush();

        producer.close();
    }
}
