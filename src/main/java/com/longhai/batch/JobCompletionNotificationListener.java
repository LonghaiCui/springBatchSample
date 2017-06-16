package com.longhai.batch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.longhai.model.Transformed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            List<Transformed> results = jdbcTemplate.query("SELECT name, date_created FROM transformed", new RowMapper<Transformed>() {
                @Override
                public Transformed mapRow(ResultSet rs, int row) throws SQLException {
                    return new Transformed(rs.getString(1), rs.getDate(2));
                }
            });

            log.info("Checking step1: transformed records in the database");

            results.forEach(result-> {
                log.info("Full name " + result.getName() + " is created on " + result.getDateCreated());
            });

            log.info("Checking step2: final file is written to \\springBatchSample\\build\\resources\\main\\output.txt");
        }
    }
}