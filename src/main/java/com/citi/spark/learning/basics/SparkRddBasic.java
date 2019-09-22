package com.citi.spark.learning.basics;

import com.citi.spark.learning.util.Util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
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
