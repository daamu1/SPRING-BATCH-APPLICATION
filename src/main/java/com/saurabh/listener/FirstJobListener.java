package com.saurabh.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FirstJobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Before Job " + jobExecution.getJobInstance().getJobName());
        log.info("Job Params " + jobExecution.getJobParameters());
        log.info("Job Exec Context " + jobExecution.getExecutionContext());

        jobExecution.getExecutionContext().put("jec", "jec value");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("After Job " + jobExecution.getJobInstance().getJobName());
        log.info("Job Params " + jobExecution.getJobParameters());
        log.info("Job Exec Context " + jobExecution.getExecutionContext());
    }

}