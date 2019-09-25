package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Filters implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {
        context.textFile("src\\main\\resources\\inputs\\biglog.txt")
                .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                .filter(word -> word.contains(","))
                .map(word -> word.replace("," , " -> "))
                .take(10)
                .forEach(System.out::println);
    }
}
