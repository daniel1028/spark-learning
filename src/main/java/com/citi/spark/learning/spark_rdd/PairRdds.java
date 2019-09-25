package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PairRdds implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {
        JavaRDD<String> intitialRdd = context.textFile("src\\main\\resources\\inputs\\biglog.txt");
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
