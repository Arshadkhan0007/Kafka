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

        // Address of the Kafka broker that this consumer will connect to.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Consumer Group ID.
        // Consumers with the same group ID share the work (partitions).
        // Kafka also stores offsets separately for each group.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "practice-group");

        // 1. Set up the JsonDeserializer
        // Kafka stores messages as byte[].
        // JsonDeserializer converts the JSON payload into a Course object.
        JsonDeserializer<Course> jsonDeserializer = new JsonDeserializer<>(Course.class);
        jsonDeserializer.addTrustedPackages("com.example.producer.model"); // Spring Kafka only deserializes classes from trusted packages for security reasons.

        // -------------------------------------------------------------
        // STEP 2: Handling ClassNotFoundException
        // -------------------------------------------------------------
        // By default, Spring Kafka adds a "__TypeId__" header
        // containing the producer's full class name.
        //
        // Example:
        // Producer package:
        //     com.example.producer.model.Course
        //
        // Consumer package:
        //     com.example.consumer.model.Course
        //
        // Without this mapping, the consumer tries:
        // Class.forName("com.example.producer.model.Course")
        //
        // Since that class doesn't exist in this project,
        // it throws ClassNotFoundException.
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        Map<String, Class<?>> idClassMapping = new HashMap<>();
        // Maps the producer's class name to the local Course class.
        idClassMapping.put("com.example.producer.model.Course", Course.class);
        // Register the mapping with the type mapper.
        typeMapper.setIdClassMapping(idClassMapping);
        // Tell the JsonDeserializer to use our custom type mapper.
        jsonDeserializer.setTypeMapper(typeMapper);

        // 3. Wrap in ErrorHandlingDeserializer to prevent IllegalStateException
        // If a message is "poison" (malformed JSON), a standard deserializer will loop infinitely.
        // ErrorHandlingDeserializer catches the exception so the consumer can move to the next message.
        ErrorHandlingDeserializer<Course> errorHandlingValueDeserializer =
                new ErrorHandlingDeserializer<>(jsonDeserializer);

        // Key Deserializer:
        // Converts Kafka message key (byte[]) -> String
        //
        // Value Deserializer:
        // Converts Kafka message value (JSON byte[])
        //      -> ErrorHandlingDeserializer
        //          -> JsonDeserializer
        //              -> Course object
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
