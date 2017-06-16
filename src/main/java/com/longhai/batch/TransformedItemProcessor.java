package com.longhai.batch;

import com.longhai.model.Person;
import com.longhai.model.Transformed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.sql.Date;


public class TransformedItemProcessor implements ItemProcessor<Transformed, Transformed> {

    private static final Logger log = LoggerFactory.getLogger(TransformedItemProcessor.class);

    @Override
    public Transformed process(final Transformed transformed) throws Exception {
        final Transformed transformedPerson = new Transformed(transformed.getName().toUpperCase(), new Date(System.currentTimeMillis()));

        log.info("Changing transformed person name to upper case (" + transformedPerson + ")");

        return transformedPerson;
    }

}