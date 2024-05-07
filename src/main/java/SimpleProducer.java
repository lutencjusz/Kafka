import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Producent Kafka
 * Wysyła 10 wiadomości do tematu "my-topic"
 * <p>
 * Aby uruchomić Kafkę, należy w terminalu wpisać:
 * cd C:\Kafka\kafka_2.13-3.7.0
 * .\bin\windows\zookeeper-server-start.bat "C:\Kafka\kafka_2.13-3.7.0\config\zookeeper.properties"
 * <p>
 * w drugim terminalu:
 * cd C:\Kafka\kafka_2.13-3.7.0
 * .\bin\windows\kafka-server-start.bat "C:\Kafka\kafka_2.13-3.7.0\config\server.properties"
 */
public class SimpleProducer {

    private final static String TOPIC_NAME = "producer-example";
    private final static int PRODUCER_MESSAGES_MAX = 200;

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SimpleProducer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, 2);

        Producer<String, Product> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < PRODUCER_MESSAGES_MAX; i++) {
                String message = "Message " + i;
                producer.send(new ProducerRecord<>(TOPIC_NAME, null, new Product(message, 1)));
                System.out.println("Sent:" + message);
            }

            producer.send(new ProducerRecord<>(TOPIC_NAME, 3, "testowy-klucz-2", new Product("Produkt testowy 1 na partycji 3", 1.1))).get();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "testowy-klucz-3", new Product("Produkt testowy 2", 1.2)), (recordMetadata, e) -> {
                if (e != null) {
                    System.out.println("Błąd wysyłania wiadomości: " + e.getMessage());
                } else {
                    System.out.printf("Wysłano komunikat:(topic: %s, partition: %s, offset: %s) ", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}
