package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import com.citi.spark.learning.model.IntegersWithSqrt;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Service
public class Tuples implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {

        //In general
        JavaRDD<IntegersWithSqrt> rddObjects = connectors.getSparkContext().parallelize(getNumbers())
                .map(num -> new IntegersWithSqrt(num));
        ;
        rddObjects.collect().forEach(numObj -> System.out.println(numObj.toString()));
        //Recommendation in spark code
        JavaRDD<Tuple2<Integer, Double>> rddTuples = connectors.getSparkContext().parallelize(getNumbers())
                .map(num -> new Tuple2<Integer, Double>(num, Math.sqrt(num)));
        rddTuples.collect().forEach(System.out::println);

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
