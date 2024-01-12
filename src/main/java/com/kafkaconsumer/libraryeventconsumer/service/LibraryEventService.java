package com.kafkaconsumer.libraryeventconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.libraryeventconsumer.entity.LibraryEvent;
import com.kafkaconsumer.libraryeventconsumer.jpa.LibraryEventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;
import java.util.Optional;

@Service
@Slf4j
@AllArgsConstructor
public class LibraryEventService {

    ObjectMapper objectMapper;

    private LibraryEventRepository libraryEventRepository;
    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
            throws JsonProcessingException {

       LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
       log.info("libraryEvent :{}", libraryEvent);

        if(libraryEvent.getLibraryEventId()!=null && (libraryEvent.getLibraryEventId()==999)){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

       switch (libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
               log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {

        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalStateException("Library Event ID missing");
        }
        Optional<LibraryEvent> optionalLibraryEvent = libraryEventRepository
                                       .findById(libraryEvent.getLibraryEventId());

        if(!optionalLibraryEvent.isPresent()){
            throw new IllegalStateException("Not a valid validation");
        }
        log.info("Validation is successful for the library Event : {} ", optionalLibraryEvent.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
    }
}
