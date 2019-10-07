package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class Reduces implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {

        int reducedVal = connectors.getSparkContext().parallelize(getNumbers())
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
