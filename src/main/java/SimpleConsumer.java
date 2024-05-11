import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Konsument Kafka
 * Odbiera wiadomości z tematu "my-topic"
 * <p>
 * Aby uruchomić Kafkę, należy w terminalu wpisać:
 * cd C:\Kafka\kafka_2.13-3.7.0
 * .\bin\windows\zookeeper-server-start.bat "C:\Kafka\kafka_2.13-3.7.0\config\zookeeper.properties"
 * <p>
 * w drugim terminalu:
 * cd C:\Kafka\kafka_2.13-3.7.0
 * .\bin\windows\kafka-server-start.bat "C:\Kafka\kafka_2.13-3.7.0\config\server.properties"
 */

public class SimpleConsumer {

    final static Long DURATION_POOL_IN_MILLIS = 100L;
    final static int CONSUMER_MESSAGES_MAX = 5;
    final static String TOPIC_NAME = "producer-example";


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, CONSUMER_MESSAGES_MAX);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");


        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(DURATION_POOL_IN_MILLIS));
                if (!records.isEmpty()) {
                    System.out.println("Messages:" + records.count());
                }
                for (var record : records) {
                    System.out.printf("Received message: (timestamp: %tT, key: %s, value: %s, partition: %d, offset: %d)\n",
                            record.timestamp(), record.key(), record.value(), record.partition(), record.offset());
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}

