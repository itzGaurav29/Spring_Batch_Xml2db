package com.example.batchprocessing;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.oxm.xstream.XStreamMarshaller;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	// end::setup[]

	// tag::readerwriterprocessor[]

	//reading from xml
	@Bean
	public StaxEventItemReader<Person> reader() {
		return new StaxEventItemReaderBuilder<Person>()
				.name("itemReader")
				.resource(new ClassPathResource("sampleXml.xml"))
				.addFragmentRootElements("person")
				.unmarshaller(personMarshaller())
				.build();

	}

	@Bean
	public XStreamMarshaller personMarshaller() {
		Map<String, Class> aliases = new HashMap<>();
		aliases.put("person", Person.class);
		aliases.put("firstName", String.class);
		aliases.put("lastName", String.class);
	
		XStreamMarshaller marshaller = new XStreamMarshaller();

		marshaller.setAliases(aliases);

		return marshaller;
	}
	
	// dummy processor to transform values to upper case
	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	// write to hsql db
	@Bean
	public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Person>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
			.dataSource(dataSource)
			.build();
	}

	// tag::jobstep[]
	@Bean
	public Job importUserJob(Step step1) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())
			.flow(step1)
			.end()
			.build();
	}

	// step 1 for reading from xml and writing to db
	@Bean
	public Step step1(JdbcBatchItemWriter<Person> writer) {
		return stepBuilderFactory.get("step1")
			.<Person, Person> chunk(10)
			.reader(reader())
			.processor(processor())
			.writer(writer)
			.build();
	}
	
	
	// step 2 for reading from db and writing to cache
	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2")
			.<Person, Person> chunk(10)
			.reader(dbReader())
		//	.processor(map2ListProcessor())
			.writer(cacheWriter())
			.build();
	}

	// read from db and store to row mapper
	@Bean
	public JdbcCursorItemReader<Person> dbReader() {
		
		JdbcCursorItemReader<Person> jdbcCursorItemReader = new JdbcCursorItemReader<>();
		jdbcCursorItemReader.setDataSource(dataSource);
		jdbcCursorItemReader.setSql("SELECT first_name, last_name FROM people");
		jdbcCursorItemReader.setRowMapper(new PersonRowMapper());
		return jdbcCursorItemReader;

	}
	
	// write to a flat file
	@Bean
	public FlatFileItemWriter<Person> cacheWriter() {
		
		FlatFileItemWriter<Person> flatFileItemWriter = new FlatFileItemWriter<>();
		flatFileItemWriter.setResource(new  ClassPathResource("sample-data.csv"));
		
		
		DelimitedLineAggregator<Person> lineAgg = new DelimitedLineAggregator<>();
		lineAgg.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Person> fieldExt = new BeanWrapperFieldExtractor<>();
		fieldExt.setNames(new String[] {"firstName","lastName"});
		lineAgg.setFieldExtractor(fieldExt);
		
		flatFileItemWriter.setLineAggregator(lineAgg);
		return flatFileItemWriter;
	}
	
	@Bean
	public Job exportPersonJob() {
		return jobBuilderFactory.get("exportPersonJob").incrementer(new RunIdIncrementer()).
				flow(step2()).end().build();
	}
	
	// processor to add objects to a list -- not working
	@Bean
	public Map2ListProcessor map2ListProcessor() {
		return new Map2ListProcessor();
	}
	
}
