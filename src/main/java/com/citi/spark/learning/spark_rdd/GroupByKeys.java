package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

//This operation is not recommended. This will massively affect the performance
public class GroupByKeys implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {
        context.textFile("src\\main\\resources\\inputs\\biglog.txt")
                .mapToPair(sentences -> new Tuple2<String, Integer>(sentences.split(",")[0], 1))
                .groupByKey()
                .take(10)
                .forEach(data -> System.out.println(data
                        ._1 + "-> " + Iterables.size(data._2)));
    }
}
