package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Maps implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {
        JavaRDD<Double> enrichedData = context.parallelize(getDoubleNumbers())
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
