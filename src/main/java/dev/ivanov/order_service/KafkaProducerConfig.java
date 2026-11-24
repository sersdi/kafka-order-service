package dev.ivanov.order_service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public ProducerFactory<String, Order> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> configProperties = new HashMap<>();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Serializer<Order> orderSerializer = (topic, order) -> {
            if (order == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(order);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Cannot serialize order", e);
            }
        };

        return new DefaultKafkaProducerFactory<>(
                configProperties,
                new StringSerializer(),
                orderSerializer
        );
    }

    @Bean
    public KafkaTemplate<String, Order> kafkaTemplate(
            ProducerFactory<String, Order> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }
}
