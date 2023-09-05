package com.saurabh.config;

import com.saurabh.entity.Customer;
import com.saurabh.listener.FirstJobListener;
import com.saurabh.listener.FirstStepListener;
import com.saurabh.repository.CustomerRepository;
import com.saurabh.service.SecondTasklet;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.beans.factory.annotation.Qualifier;

@Configuration
@EnableBatchProcessing
//@AllArgsConstructor
public class SpringBatchConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final CustomerRepository customerRepository;

    private final FirstJobListener firstJobListener;

    private final FirstStepListener firstStepListener;

    private final SecondTasklet secondTasklet;
    @Autowired
    public SpringBatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, CustomerRepository customerRepository, FirstJobListener firstJobListener, FirstStepListener firstStepListener, SecondTasklet secondTasklet) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.customerRepository = customerRepository;
        this.firstJobListener = firstJobListener;
        this.firstStepListener = firstStepListener;
        this.secondTasklet = secondTasklet;
    }
    /*
    Tasklet Processing:

    Description: Tasklet processing is a more straightforward and flexible approach in Spring Batch.
    How it works: In tasklet processing, a single unit of work, known as a "tasklet," is executed. Tasklets can perform any custom logic or processing needed.
    Use Cases: Tasklet processing is suitable for scenarios where the processing logic doesn't fit well into the chunk-oriented model. It allows you to perform custom tasks, such as sending emails, calling APIs, or performing database maintenance.
    Components: Tasklet processing mainly involves a single component:
    Tasklet: A tasklet is a Java class that implements the org.springframework.batch.core.step.tasklet.Tasklet interface, defining the custom logic to be executed.
    */

    @Bean
    @Qualifier("firstJob")
    public Job firstJob() {
        return jobBuilderFactory
                .get("First Job")
                .incrementer(new RunIdIncrementer())
                .start(firstStep())
                .next(secondStep())
                .listener(firstJobListener)
                .build();
    }

    private Step firstStep() {
        return stepBuilderFactory
                .get("First Step")
                .tasklet(firstTask())
                .listener(firstStepListener)
                .build();
    }

    private Tasklet firstTask() {
        return new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("This is first tasklet step");
                System.out.println("SEC = " + chunkContext.getStepContext().getStepExecutionContext());
                return RepeatStatus.FINISHED;
            }
        };
    }

    private Step secondStep() {
        return stepBuilderFactory
                .get("Second Step")
                .tasklet(secondTasklet)
                .build();
    }

    /*
    from here to below code is  for csv to sql database
    Chunk Processing:

    Description: Chunk processing is the most common and widely used processing type in Spring Batch.
    How it works: In chunk processing, data is read in chunks (usually a configurable number of items) from a source (like a file or database), then processed and written to a destination in the same-sized chunks.
    Use Cases: Chunk processing is suitable for scenarios where data can be divided into manageable pieces and processed in parallel, such as ETL (Extract, Transform, Load) processes or bulk data processing.
    Components: Chunk processing typically involves three key components:
    Item Reader: Reads data in chunks from a source.
    Item Processor: Processes each chunk of data.
    Item Writer: Writes the processed data in chunks to a destination.
    */
    @Bean
    public FlatFileItemReader<Customer> reader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("CSV READER");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;

    }

    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    @Bean
    public RepositoryItemWriter<Customer> writer() {
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory
                .get("CSV STEP 1")
                .<Customer, Customer>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    @Primary
    public Job runJob() {
        return jobBuilderFactory
                .get("IMPORT CUSTOMER")
                .flow(step1())
                .end()
                .build();

    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(100);
        return asyncTaskExecutor;
    }

}
