package com.saurabh.service;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class JobService {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    @Qualifier("firstJob")
    Job firstJob;

    @Qualifier("runJob")
    @Autowired
    Job runJob;

    @Async
    public void startJob(String jobName) {
        Map<String, JobParameter> params = new HashMap<>();
        params.put("currentTime", new JobParameter(System.currentTimeMillis()));
        JobParameters jobParameters = new JobParameters(params);
        try {
            JobExecution jobExecution = null;
            if (jobName.equals("First Job")) {
                jobExecution = jobLauncher.run(firstJob, jobParameters);
            } else if (jobName.equals("IMPORT CUSTOMER")) {
                jobExecution = jobLauncher.run(runJob, jobParameters);
            }
            assert jobExecution != null;
            log.info("Job Execution ID = " + jobExecution.getId());
        } catch (Exception e) {
            log.info("Exception while starting job");
        }
    }

}