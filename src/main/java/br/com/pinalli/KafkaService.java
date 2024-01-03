package br.com.pinalli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

 class KafkaService {          
        private final KafkaConsumer<String, String> consumer;
        private final ConsumerFunction parse;

        // Construtor que recebe o tópico e a função de consumo como parâmetros
       KafkaService(String topic, ConsumerFunction parse) {
            this.parse = parse; // Initialize the parse field
            this.consumer = new KafkaConsumer<String, String>(properties());          
            consumer.subscribe(Collections.singletonList(topic));
           
        }

        void run(){
             while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " records");
                    for (var record : records) {
                        parse.consume(record);
                        
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
        return properties;
    }
}
