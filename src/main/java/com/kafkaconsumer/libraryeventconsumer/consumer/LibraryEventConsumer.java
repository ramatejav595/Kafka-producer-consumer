package com.kafkaconsumer.libraryeventconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.libraryeventconsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventConsumer {

    private LibraryEventService libraryEventService;

    public LibraryEventConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer Record : {}", consumerRecord);
        log.info("New record");
        libraryEventService.processLibraryEvent(consumerRecord);
    }
}
