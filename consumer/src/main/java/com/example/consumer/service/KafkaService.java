package com.example.consumer.service;

import com.example.consumer.model.Course;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaService {

    private String message;

    // If you want to read from a particular partition
    // @KafkaListener(
    //      topics = "practice-topic-1",
    //      groupId = "practice-group-1",
    //      topicPartitions = {@TopicPartition(topic = "practice-topic-1", partition = {"2})}
    // )

    @RetryableTopic(
            attempts = "4",
//            exclude = {NullPointerException.class},
            backoff = @Backoff(delay = 2000)
    )
    @KafkaListener(topics = "practice-topic", groupId = "practice-group-1")
    public void getMessage1(Course course) {
        message = course.toString();
        if(course.getCourseId().equals("COURSE-101")) throw new RuntimeException("Invalid course ID");
        log.info("Received data on consumer-1 : {}", message);
    }

    @DltHandler
    public void listenDLT(Course course, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("DLT Received : {}, from {}, offset {}", course.toString(), topic, offset);
    }

}
