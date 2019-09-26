package com.citi.spark.learning.spark_rdd;

import com.citi.spark.learning.connectors.SparkContextConnector;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Joins implements SparkContextConnector {
    @Override
    public void execute(JavaSparkContext context) {

        List<Tuple2<Integer, Integer>> userVisits = new ArrayList<>();
        userVisits.add(new Tuple2<>(1, 10));
        userVisits.add(new Tuple2<>(2, 18));
        userVisits.add(new Tuple2<>(3, 25));
        userVisits.add(new Tuple2<>(4, 15));
        userVisits.add(new Tuple2<>(5, 20));

        List<Tuple2<Integer, String>> userRow = new ArrayList<>();
        userRow.add(new Tuple2<>(1, "Daniel"));
        userRow.add(new Tuple2<>(2, "Raja"));
        userRow.add(new Tuple2<>(3, "Suresh"));
        userRow.add(new Tuple2<>(4, "Vijay"));
        userRow.add(new Tuple2<>(5, "Lawerence"));

        JavaPairRDD<Integer, Integer> userVisitsRdd = context.parallelizePairs(userVisits);
        JavaPairRDD<Integer, String> userRowsRdd = context.parallelizePairs(userRow);

        //Inner join
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinedRdd = userRowsRdd.join(userVisitsRdd);
        joinedRdd.take(10).forEach(u -> System.out.println(u._1 + "->" + u._2._1 + " visits " + u._2._2 + " times"));

        //Left Join
        JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> leftOuterJoin = userRowsRdd.leftOuterJoin(userVisitsRdd);
        //Right Join
        JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightOuterJoin = userRowsRdd.rightOuterJoin(userVisitsRdd);
        // Full outer join
        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> fullOuterJoin = userRowsRdd.fullOuterJoin(userVisitsRdd);



        leftOuterJoin.take(10).forEach(System.out::println);
        rightOuterJoin.take(10).forEach(System.out::println);
        fullOuterJoin.take(10).forEach(System.out::println);
    }
}
