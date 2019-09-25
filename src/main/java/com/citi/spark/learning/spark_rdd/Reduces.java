package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Reduces implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {

        int reducedVal = context.parallelize(getNumbers())
                .reduce((v1, v2) -> (v1 = v2))//Use lambda Function2
                ;
        System.out.println("Final Output : " + reducedVal);

    }

    private List<Integer> getNumbers() {
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(25);
        numbers.add(45);
        numbers.add(23);
        return numbers;
    }
}
