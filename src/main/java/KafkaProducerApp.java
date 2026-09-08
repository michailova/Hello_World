import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "events",
                            "key-" + i,
                            "Hello Kafka " + i
                    );

            producer.send(record);
        }

        producer.close();
    }
}
