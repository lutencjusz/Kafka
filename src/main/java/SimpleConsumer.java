import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

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
    public static void main(String[] args) {
        String topicName = "producer-example";
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (var record : records) {
                    System.out.printf("Received message: (timestamp: %tT, key: %s, value: %s, partition: %d, offset: %d)\n",
                           record.timestamp(), record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}

