package com.example.producer.configuration;

import com.example.producer.model.Course;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.topic-name}")
    private String topicName;

    /**
     * This bean tells Spring to automatically create the topic on the Kafka broker
     * if it doesn't already exist.
     */
    @Bean
    public NewTopic createTopic() {
        // Parameters: (Topic Name, Number of Partitions, Replication Factor)
        return new NewTopic(topicName, 5, (short) 2);
    }

    /**
     * ProducerFactory is responsible for creating Kafka Producer instances.
     * It holds the technical configuration (Server address, Serializers, etc).
     */
    @Bean
    public ProducerFactory<String, Course> producerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // 1. RETRIES: "If the delivery fails, how many times should I try again?"
        // Setting this to MAX_VALUE means "Keep trying as many times as possible."
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // 2. DELIVERY TIMEOUT: "What is the absolute maximum time I'm willing to wait for a successful delivery?"
        // Even though retries are set to "infinite" above, this puts a hard 2-minute (120,000 ms) stop on the whole process.
        // If 2 minutes pass, stop retrying and throw an error back to the application.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        // 3. RETRY BACKOFF: "How long should I wait between failed attempts?"
        // If the broker is down, don't spam it immediately. Wait 1 second (1000 ms) before trying to send the message again.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        // 4. ACKS (Acknowledgments): "Who needs to sign for this package before I consider it successfully delivered?"
        // "all" means the main Kafka broker AND all of its backup brokers must save the message to their hard drives before telling you "success." It's the safest setting.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 5. ENABLE IDEMPOTENCE: "How do I prevent accidentally sending duplicate messages?"
        // If the broker saves your message, but the "success" receipt gets lost in the network, your producer will retry sending it.
        // Setting this to 'true' gives the message a unique ID. If Kafka sees a retry with the same ID, it safely ignores the duplicate.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 6. MAX IN-FLIGHT REQUESTS: "How many unacknowledged messages can be in the air at the same time?"
        // We set this to 5. It allows Kafka to send up to 5 messages over the network at once without waiting for their receipts, which speeds things up.
        // (Note: Kafka guarantees that using 5 or fewer keeps your messages in the exact order you sent them when idempotence is true).
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Course> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
