package br.com.pinalli;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        try (var producer = new KafkaProducer<String, String>(properties())) {
            var value = "7889,34455,8787879797";
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                // observer
                System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "
                        + data.offset() + "/ timestamp " + data.timestamp());
            };
            var email = "Thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", email, email);
            producer.send(record, callback).get();
            producer.send(emailRecord, callback).get();
            producer.close(); // Fechar o produtor ap�s o envio
        }
    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
