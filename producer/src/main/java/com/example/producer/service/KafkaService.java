package com.example.producer.service;

import com.example.producer.model.Course;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaService {

    private final KafkaTemplate<String, Course> kafkaTemplate;
    private final String topicName;

    public KafkaService(KafkaTemplate<String, Course> kafkaTemplate, @Value("${spring.topic-name}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void receiveMessage(Course course) {
        CompletableFuture<SendResult<String, Course>> future = kafkaTemplate.send(topicName, course.getCourseId(), course);

        // If you want to send the event to a particular partition
        // kafkaTemplate.send(topicName, 4, course.getCourseId(), course);

        future.whenComplete((result, ex) -> {
            if(ex == null) {
                System.out.printf("Message has been published successfully | Data: %s | Partition: %s | Offset: %s%n", result.getProducerRecord().value(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                System.out.printf("Unable to send message, Reason: %s%n", ex.getMessage());
            }
        });

    }



}
