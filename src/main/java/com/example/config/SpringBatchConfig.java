package com.example.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.example.entity.Customer;
import com.example.repo.CustomerRepo;

import lombok.AllArgsConstructor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {
	
	private JobBuilderFactory jobBuilderFactory;
	private StepBuilderFactory stepBuilderFactory;
	private CustomerRepo customerRepository;
	
	@Bean
	public FlatFileItemReader<Customer> customerReader() {
		FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();  // it is used to read data from csv
		itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));  // to define source path
		itemReader.setName("csv-reader");  // giving a name to item reader
		itemReader.setLinesToSkip(1);  // we will have columns names in the table, so we can skip 1 line
		itemReader.setLineMapper(lineMapper());
		return itemReader;
	}
	
	//line mapper is a component that maps lines from a flat file (like CSV or text files) to Java objects.

	private LineMapper<Customer> lineMapper() {
		 DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
		 DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();  // split lines of text into fields/tokens
		 lineTokenizer.setDelimiter(","); // seperated by comma
			lineTokenizer.setStrict(false);  // if the value is not there, it will insert null; if it is true, then it will throw some error
			lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
			//names of fields corresponding tokens
			
			/*BeanWrapperFieldSetMapper<Customer>: This creates a mapper that maps the fields
			extracted from a line (represented as a FieldSet) to properties of a 
			Java object (in this case, a Customer object) using Java Bean conventions.*/

			BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
			fieldSetMapper.setTargetType(Customer.class); //

			lineMapper.setLineTokenizer(lineTokenizer);
			// when the line mapper processes a line, it will use this tokenizer to split the line into fields.
			lineMapper.setFieldSetMapper(fieldSetMapper);
			//ine mapper to use the BeanWrapperFieldSetMapper to map the tokens extracted from the line into a Customer object.

			
		 
		return lineMapper;
	}
	
	@Bean
	public CustomerProcessor customerProcessor() {
		return new CustomerProcessor();
	}
	
	@Bean
	public RepositoryItemWriter<Customer> customerWriter() {

		RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
		writer.setRepository(customerRepository);
		writer.setMethodName("save");

		return writer;
	}
	
	@Bean
	public Step step() {
		return stepBuilderFactory.get("step-1").<Customer, Customer>chunk(10)
						  .reader(customerReader())
						  .processor(customerProcessor())
						  .writer(customerWriter())
						  .taskExecutor(taskExecutor())
						  .build();
	}
	
	@Bean
	public Job job() {
		return jobBuilderFactory.get("customers-import")
								.flow(step())
								.end()
								.build();
	}
	
	@Bean
	public TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(10);
		return taskExecutor;
	}
	
}
