package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class Maps implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        JavaRDD<Double> enrichedData = connectors.getSparkContext().parallelize(getDoubleNumbers())
                .map(doubleVal -> doubleVal + 10.0);
        enrichedData.collect() //collect fun will collect the data from all the nodes
                .forEach(System.out::println);

    }


    private List<Double> getDoubleNumbers() {
        List<Double> doubles = new ArrayList<>();
        doubles.add(10.10);
        doubles.add(23.20);
        doubles.add(12.33);
        doubles.add(45.22);
        return doubles;
    }
}
