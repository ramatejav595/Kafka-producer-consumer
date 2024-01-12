package com.kafkaconsumer.libraryeventconsumer.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.libraryeventconsumer.config.LibraryEventConsumerConfig;
import com.kafkaconsumer.libraryeventconsumer.entity.FailureRecord;
import com.kafkaconsumer.libraryeventconsumer.jpa.FailureRecordRepository;
import com.kafkaconsumer.libraryeventconsumer.service.LibraryEventService;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class RetryScheduler {

    public LibraryEventService libraryEventService;
    public FailureRecordRepository failureRecordRepository;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {

        var status = LibraryEventConsumerConfig.RETRY;
        failureRecordRepository.findAllByStatus(status).forEach(failureRecord -> {
                var customerRecord = buildFailedRecords(failureRecord);
                    try {
                         libraryEventService.processLibraryEvent(customerRecord);
                         failureRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }  }
                    );
        }
    private ConsumerRecord<Integer,String> buildFailedRecords(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(), failureRecord.getOffset_value(), failureRecord.getKey_value(),
                failureRecord.getErrorRecord());
    }
}
