package com.kafkaconsumer.libraryeventconsumer.config;

import com.kafkaconsumer.libraryeventconsumer.service.FailureRecordService;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Configuration
@Slf4j
public class LibraryEventConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String SUCCESS = "SUCCESS";
    public static final String DEAD = "DEAD";

    private KafkaTemplate kafkaTemplate;

    private FailureRecordService failureRecordService;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate
                , (r, e) -> {
            log.error("Exception in publishing recovered : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        }
        );
    }

    public DefaultErrorHandler errorHandler() {

        var exceptions = List.of(IllegalStateException.class);
        var fixedBackOff = new FixedBackOff(1000L, 4);

        var defaultErrorHandler = new DefaultErrorHandler(consumerRecordRecoverer, fixedBackOff);

        exceptions.forEach(defaultErrorHandler::addNotRetryableExceptions);

        defaultErrorHandler.setRetryListeners((record, ex, deliveryAttempt) -> log.info("Failed Record in " +
                "Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt));
        return defaultErrorHandler;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, exception) -> {
        log.info("Exception is: {} and Failed record: {}", exception, consumerRecord);

        if (exception.getCause().equals("RecoverableDataAccessException")) {
            failureRecordService.processFailedLibraryEvent((ConsumerRecord<Integer, String>) consumerRecord,
                    exception, RETRY);
        } else {   
            log.info("Inside the non recoverable logic and skipping the record : {} ", consumerRecord);
        }
    };


    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new
                ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setCommonErrorHandler(errorHandler());
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }
}
