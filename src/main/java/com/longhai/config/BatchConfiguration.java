package com.longhai.config;

import com.longhai.batch.JobCompletionNotificationListener;
import com.longhai.batch.PersonItemProcessor;
import com.longhai.batch.TransformedItemProcessor;
import com.longhai.model.Transformed;
import com.longhai.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

import static java.util.Arrays.asList;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Bean
    ItemReader<Person> reader1(DataSource dataSource) {
        JdbcCursorItemReader<Person> databaseReader = new JdbcCursorItemReader<>();

        databaseReader.setDataSource(dataSource);
        databaseReader.setSql("select first_name, last_name from people;");
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));

        return databaseReader;
    }

    @Bean
    public PersonItemProcessor processor1() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Transformed> writer1() {
        JdbcBatchItemWriter<Transformed> writer = new JdbcBatchItemWriter<Transformed>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Transformed>());
        writer.setSql("INSERT INTO transformed (name, date_created) VALUES (:name, :dateCreated)");
        writer.setDataSource(dataSource);
        return writer;
    }
    // end::readerwriterprocessor[]

    @Bean
    public Step step1() {
        TaskletStep step1 = stepBuilderFactory.get("step1")
                .<Person, Transformed>chunk(10)
                .reader(reader1(dataSource))
                .processor(processor1())
                .writer(writer1())
                .build();

        return  step1;
    }

    @Bean
    ItemReader<Transformed> reader2(DataSource dataSource) {
        JdbcCursorItemReader<Transformed> databaseReader = new JdbcCursorItemReader<>();

        databaseReader.setDataSource(dataSource);
        databaseReader.setSql("select name, date_created from transformed;");
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Transformed.class));

        return databaseReader;
    }

    @Bean
    public TransformedItemProcessor processor2() {
        return new TransformedItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<Transformed> writer2() {
//        FlatFileItemWriter<Transformed> writer = new FlatFileItemWriter<Transformed>();
//        writer.setResource(new ClassPathResource("output.txt"));
//        writer.setLineAggregator(new DelimitedLineAggregator() {{
//            setFieldExtractor(new BeanWrapperFieldExtractor<Transformed>(){{
//                String[] names = new String[2];
//                names[0] = "name";
//                names[1] = "dateCreated";
//                setNames(names);
//            }});
//            setDelimiter("|");
//        }});

        FlatFileItemWriter<Transformed> writer = new FlatFileItemWriter<Transformed>();
        writer.setResource(new ClassPathResource("output.txt"));
        DelimitedLineAggregator<Transformed> delLineAgg = new DelimitedLineAggregator<Transformed>();
        delLineAgg.setDelimiter(",");
        BeanWrapperFieldExtractor<Transformed> fieldExtractor = new BeanWrapperFieldExtractor<Transformed>();
        fieldExtractor.setNames(new String[] {"name", "dateCreated"});
        delLineAgg.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(delLineAgg);

        writer.close();
        return writer;
    }

    @Bean
    public Step step2() {
        TaskletStep step2 = stepBuilderFactory.get("step2")
                .<Transformed, Transformed>chunk(1)
                .reader(reader2(dataSource))
                .processor(processor2())
                .writer(writer2())
                .build();
        return step2;
    }


    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step1())
                .next(step2())
                .build();
    }

//    @Bean
//    public Job exportUserJob(JobCompletionNotificationListener listener) {
//        return jobBuilderFactory.get("exportUserJob")
//                .incrementer(new RunIdIncrementer())
//                .listener(listener)
//                .flow(step2())
//                .end()
//                .build();
//    }

}