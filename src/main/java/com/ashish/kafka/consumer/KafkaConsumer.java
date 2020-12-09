package com.ashish.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.sql.Timestamp;

@Service
public class KafkaConsumer {
    @KafkaListener(topics = "ashish", groupId = "mondal", containerFactory = "kafkaListenerContainerFactory")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            Acknowledgment ack) {
        System.out.println( new Timestamp(new java.util.Date().getTime()) +
                " Received Message: " + message
                        + " from partition: " + partition);
        try {
            Thread.sleep(2000);
            System.out.println( new Timestamp(new java.util.Date().getTime()) + " Finished processing======");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ack.acknowledge();
        }
    }

    //@KafkaListener(topics = "ashish", groupId = "mondal", containerFactory = "kafkaListenerContainerFactory")
    public void listenWithHeaders1(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            Acknowledgment ack) {
        System.out.println( new Timestamp(new java.util.Date().getTime()) +
                " Received Message: " + message
                + " from partition: " + partition);
        try {
            Thread.sleep(2000);
            System.out.println( new Timestamp(new java.util.Date().getTime()) + " Finished processing======");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ack.acknowledge();
        }
    }
}
