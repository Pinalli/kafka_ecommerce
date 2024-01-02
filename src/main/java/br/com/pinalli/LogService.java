package br.com.pinalli;

import java.time.Duration;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogService {
     public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        while (true) {
            var records = consumer.poll(Duration.ofMillis(1000));

            if (!records.isEmpty()) {
                System.out.println("Find " + records.count() + " records");                

            for (var record : records) {
                System.out.println("----------------------------------------");
                System.out.println("LOG: " + record.topic());
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value());
                System.out.println("Partition: " + record.partition());
                System.out.println("Offset: " + record.offset());
        }
          }
        }
    }
        private static Properties properties() {
            
            var properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
            return properties;           
        }
}

