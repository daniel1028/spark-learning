package com.citi.spark.learning.basics;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;

@Component
public class SparkRddBasic implements SparkContextConnector {

    @Override
    public void execute(JavaSparkContext context) {
        context.textFile("C:\\Users\\esscay\\IdeaProjects\\learn-spark\\src\\main\\resources\\inputs")
                .take(50)
                .forEach(System.out::println);
    }
}
