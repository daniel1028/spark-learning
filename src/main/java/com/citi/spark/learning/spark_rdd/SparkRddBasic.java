package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class SparkRddBasic implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        connectors.getSparkContext().parallelize(getNameList()) //distributed dataset that can be operated on in parallel
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
