package com.example.consumer.controller;

import com.example.consumer.model.Course;
import com.example.consumer.service.KafkaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaService service;

    public KafkaController(KafkaService service) {
        this.service = service;
    }

//    @GetMapping("/add-course")
//    public ResponseEntity<String> getCourse(@RequestBody Course course) {
//        service.getMessage(course);
//        return ResponseEntity.ok("Message retrieved successfully");
//    }

}
