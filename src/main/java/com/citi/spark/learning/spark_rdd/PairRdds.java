package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

@Service
public class PairRdds implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        JavaRDD<String> intitialRdd = connectors.getSparkContext().textFile("src\\main\\resources\\inputs\\biglog.txt");
        JavaPairRDD<String, String> logpairs = intitialRdd.mapToPair(sentences -> {
            String columns[] = sentences.split(",");
            String level = columns[0];
            String date = columns[1];
            return new Tuple2<String, String>(level, date);
        });

        logpairs.take(10)
                .forEach(out -> System.out.println(out._1 + ":" + out._2));
    }
}
