package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

@Service
public class ReduceByKeys implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        JavaRDD<String> intitialRdd = connectors.getSparkContext().textFile("src\\main\\resources\\inputs\\biglog.txt");
        JavaPairRDD<String, Integer> logpairs = intitialRdd.mapToPair(sentences -> {
            String columns[] = sentences.split(",");
            String level = columns[0];
            return new Tuple2<String, Integer>(level, 1);
        });
        JavaPairRDD<String, Integer> aggregatedDate = logpairs.reduceByKey((v1, v2) -> (v1 + v2));
        aggregatedDate.take(10)
                .forEach(out -> System.out.println(out._1 + ":" + out._2));

        //Using Fluent API

        intitialRdd.mapToPair(sentences -> new Tuple2<String, Integer>(sentences.split(",")[0], 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .take(10)
                .forEach(data -> System.out.println(data
                        ._1 + "-> " + data._2));
    }
}
