package br.com.pinalli;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

public class NewOrderMain {

    public static void main(String[] args) {
       var producer = new KafkaProducer<String, String>(properties());
    }

    private static Properties properties() {

        var properties = new Properties();
        
        return properties;
    }
    
}
