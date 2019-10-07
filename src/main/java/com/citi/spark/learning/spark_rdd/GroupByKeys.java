package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import com.google.common.collect.Iterables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

//This operation is not recommended. This will massively affect the performance
@Service
public class GroupByKeys implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        connectors.getSparkContext().textFile("src\\main\\resources\\inputs\\biglog.txt")
                .mapToPair(sentences -> new Tuple2<String, Integer>(sentences.split(",")[0], 1))
                .groupByKey()
                .take(10)
                .forEach(data -> System.out.println(data
                        ._1 + "-> " + Iterables.size(data._2)));
    }
}
