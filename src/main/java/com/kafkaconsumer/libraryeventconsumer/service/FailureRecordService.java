package com.kafkaconsumer.libraryeventconsumer.service;

import com.kafkaconsumer.libraryeventconsumer.entity.FailureRecord;
import com.kafkaconsumer.libraryeventconsumer.jpa.FailureRecordRepository;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@NoArgsConstructor
@Slf4j

public class FailureRecordService {

    private FailureRecordRepository failureRecordRepository;

    public void processFailedLibraryEvent(ConsumerRecord<Integer, String> record, Exception exception, String status){

        var failureRecord = new FailureRecord(null,record.topic(), record.key(), record.value(),
                record.partition(),record.offset(),exception.getCause().getMessage(), status);

        failureRecordRepository.save(failureRecord);
    }

    public void processFailedLibraryEvent(FailureRecord failureRecord) {
    }
}
