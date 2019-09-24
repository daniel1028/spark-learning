package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SparkRddBasic implements SparkContextConnector {

    @Override
    public void execute(JavaSparkContext context) {
        context.parallelize(getNameList()) //distributed dataset that can be operated on in parallel
                .take(50)
                .forEach(System.out::println);

    }

    private List<String> getNameList() {
        List<String> names = new ArrayList<>();
        names.add("Daniel");
        names.add("Raja");
        names.add("Suresh");
        return names;
    }
}
