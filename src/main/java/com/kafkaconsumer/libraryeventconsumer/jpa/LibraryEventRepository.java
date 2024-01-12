package com.kafkaconsumer.libraryeventconsumer.jpa;

import com.kafkaconsumer.libraryeventconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {

    Optional<LibraryEvent> findById(Integer libraryEventId);

}
