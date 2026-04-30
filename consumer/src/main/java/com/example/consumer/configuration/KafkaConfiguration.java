package com.example.consumer.configuration;

import com.example.consumer.model.Course;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Bean
    public ConsumerFactory<String, Course> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "practice-group");

        // 1. Set up the JsonDeserializer
        JsonDeserializer<Course> jsonDeserializer = new JsonDeserializer<>(Course.class);
        jsonDeserializer.addTrustedPackages("com.example.producer.model");

        // 2. Fix ClassNotFoundException:
        // By default, Kafka sends the full class path in the header (e.g., "com.example.producer.model.Course").
        // If your consumer project has a different package structure, it will crash.
        // This mapping tells Kafka: "If you see the producer's class path, map it to my local Course class."
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        Map<String, Class<?>> idClassMapping = new HashMap<>();
        idClassMapping.put("com.example.producer.model.Course", Course.class);
        typeMapper.setIdClassMapping(idClassMapping);
        jsonDeserializer.setTypeMapper(typeMapper);

        // 3. Wrap in ErrorHandlingDeserializer to prevent IllegalStateException
        // If a message is "poison" (malformed JSON), a standard deserializer will loop infinitely.
        // ErrorHandlingDeserializer catches the exception so the consumer can move to the next message.
        ErrorHandlingDeserializer<Course> errorHandlingValueDeserializer =
                new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), errorHandlingValueDeserializer);
    }

    /**
     * This factory is what enables the @KafkaListener annotation in your code.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Course> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Course> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    /*
     * PRODUCER BEANS WITHIN CONSUMER:
     * Sometimes a consumer needs to be a producer too (e.g., for "Dead Letter Topics"
     * or "Retry Topics" to send failed messages elsewhere).
     */
    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Use JsonSerializer so the Consumer can send Course objects to the retry topic
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
