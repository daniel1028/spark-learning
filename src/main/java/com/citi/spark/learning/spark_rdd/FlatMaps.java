package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class FlatMaps implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        connectors.getSparkContext().textFile("src\\main\\resources\\inputs\\biglog.txt")
                .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                .take(10)
                .forEach(System.out::println);
    }
}
