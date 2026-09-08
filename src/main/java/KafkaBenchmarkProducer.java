import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaBenchmarkProducer {

    private static final int NUMBER_OF_EVENTS = 1_000_000;
    private static final String TOPIC = "events-1p";

    public static void main(String[] args) {

        Properties properties = new Properties();


        properties.put("bootstrap.servers", "localhost:9092");

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put("acks", "1");
        properties.put("batch.size", "65536");
        properties.put("linger.ms", "0");
        properties.put("compression.type", "zstd");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        long startTime = System.nanoTime();

        for (int i = 0; i < NUMBER_OF_EVENTS; i++) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            TOPIC,
                            "key-" + i,
                            "Hello Kafka " + i
                    );

            producer.send(record);
        }

        producer.flush();

        long endTime = System.nanoTime();

        producer.close();

        double elapsedSeconds =
                (endTime - startTime) / 1_000_000_000.0;

        double eventsPerSecond =
                NUMBER_OF_EVENTS / elapsedSeconds;

        System.out.println();
        System.out.println("===== Kafka Benchmark =====");
        System.out.println("Events: " + NUMBER_OF_EVENTS);
        System.out.printf("Time: %.3f seconds%n", elapsedSeconds);
        System.out.printf("Events/sec: %.2f%n", eventsPerSecond);
    }
}
