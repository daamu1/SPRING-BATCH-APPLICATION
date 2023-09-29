package com.saurabh.service;


import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SecondTasklet implements Tasklet {

    @Override
    public RepeatStatus execute(@NotNull StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("This is second tasklet step");
        log.info(chunkContext.getStepContext().getJobExecutionContext().toString());
        return RepeatStatus.FINISHED;
    }

}