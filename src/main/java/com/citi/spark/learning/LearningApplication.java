package com.citi.spark.learning;

import com.citi.spark.learning.config.SparkJob;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;


@SpringBootApplication
public class LearningApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(LearningApplication.class, args);
        String[] jobs = context.getBeanNamesForType(SparkJob.class);
        Arrays.asList(jobs).parallelStream().forEach(job -> {
            System.out.println("Executing : " + job);
            SparkJob sparkJob = (SparkJob) context.getBean(job);
            sparkJob.execute();
        });
    }
}
