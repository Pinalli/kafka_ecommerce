
package br.com.pinalli;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudDetectorService_copy {

    public static void main(String[] args) {
        try (var consumer = new KafkaConsumer<String, String>(properties())) {
            consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " records");

                    for (var record : records) {
                        System.out.println("----------------------------------------");
                        System.out.println("Processing new order, verificando fraude");
                        System.out.println("Key: " + record.key());
                        System.out.println("Value: " + record.value());
                        System.out.println("Partition: " + record.partition());
                        System.out.println("Offset: " + record.offset());

                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            // ignoring
                            e.printStackTrace();
                        }
                        System.out.println("Order processed");
                    }
                }
            }
        }
    }

    private static Properties properties() {

        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;     
    }
}