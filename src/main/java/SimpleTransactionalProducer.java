import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Producent Kafka
 * Wysyła wiadomości transakcyjne do tematu TOPIC_NAME
 * <p>
 * Aby uruchomić Kafkę, należy w terminalu wpisać:
 * cd C:\Kafka\kafka_2.13-3.7.0
 * .\bin\windows\zookeeper-server-start.bat "C:\Kafka\kafka_2.13-3.7.0\config\zookeeper.properties"
 * <p>
 * w drugim terminalu:
 * cd C:\Kafka\kafka_2.13-3.7.0
 * .\bin\windows\kafka-server-start.bat "C:\Kafka\kafka_2.13-3.7.0\config\server.properties"
 */
public class SimpleTransactionalProducer {

    private final static String TOPIC_NAME = "producer-example";

    private static void sendTransactionMessages(Producer<String, Product> producer, int PRODUCER_MESSAGES_MAX) {
        producer.beginTransaction();
        System.out.print("Transakcja rozpoczęta...");
        for (int i = 0; i < PRODUCER_MESSAGES_MAX; i++) {
            String message = "Message transaction ok " + i;
            try {
                if (i == 3){
                    producer.abortTransaction();
                    System.out.println("Transakcja przerwana...");
                    throw new RuntimeException("Error");
                }
                producer.send(new ProducerRecord<>(TOPIC_NAME, null, new Product(message, 1))).get();
            } catch (InterruptedException | ExecutionException e) {
                producer.abortTransaction();
                System.out.println("Transakcja przerwana z błędem: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        producer.commitTransaction();
        System.out.println("Transakcja zakończona.");
    }
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SimpleTransactionProducer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class);
//        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, Product> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        sendTransactionMessages(producer, 2);
        sendTransactionMessages(producer, 5);
        producer.close();
    }
}
