package br.com.pinalli;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        try (var producer = new KafkaProducer<String, String>(properties())) {
            var key = UUID.randomUUID().toString(); // gerar uma chave aleatória
            var value = key + ",123,456"; // criar um valor com a chave e dois números
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value); // criar um registro para o tópico
                                                                                  // ECOMMERCE_NEW_ORDER
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Estou processando novo pedido, checando por fraudes");
                // observer
                System.out.println("Success send " + data.topic() + ":::partition " + data.partition() + "/ offset "
                        + data.offset() + "/ timestamp " + data.timestamp());
            };
            var email = "Thank you for your order! We are processing your order!";
            System.out.println("Email sent!");
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL",  key, email); 
            // criar um registro para o tópico ECOMMERCE_SEND_EMAIL
            producer.send(record, callback).get(); // enviar o registro do pedido e esperar a confirmação
            producer.send(emailRecord, callback).get(); // enviar o registro do email e esperar a confirmação
            // producer.close(); // não precisa fechar o produtor, pois o try-with-resources
            // já faz isso
        }
    }

    private static Properties properties() {

        var properties = new Properties();
        // definir o endereço do broker do Kafka
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); 
        // definir o serializador da chave como String                                                                                 
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  
                                                                                                            
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 
                                                                                                               
        return properties;
    }
}