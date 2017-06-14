package com.longhai.batch;

import com.longhai.model.Person;
import com.longhai.model.Transformed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.sql.Date;

public class PersonItemProcessor implements ItemProcessor<Person, Transformed> {

    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    @Override
    public Transformed process(final Person person) throws Exception {
        final Transformed transformedPerson = new Transformed(person.getFirstName() + " " + person.getLastName(), new Date(System.currentTimeMillis()));

        log.info("Converting (" + person + ") into (" + transformedPerson + ")");

        return transformedPerson;
    }

}