package com.ashish.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.Mapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
    @Value(value = "${kafka.topic}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/health")
    public String health() {
        for(int i = 0; i < 4; i++) {
            sendMessage("test");
        }

        return "ok";
    }

    public void sendMessage(String msg) {
        kafkaTemplate.send(topicName, msg);
    }
}
