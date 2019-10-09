package com.citi.spark.learning;

import com.citi.spark.learning.config.SparkJob;
import com.citi.spark.learning.jobs.Job1;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;


@SpringBootApplication
public class LearningApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(LearningApplication.class, args);
        SparkJob sparkJob1 =  context.getBean(Job1.class);
        sparkJob1.execute();
        /*String[] jobs = context.getBeanNamesForType(SparkJob.class);
        Arrays.asList(jobs).stream().forEach(job -> {
            System.out.println("Executing : " + job);
            SparkJob sparkJob = (SparkJob) context.getBean(job);
            sparkJob.execute();
        });*/
    }
}
