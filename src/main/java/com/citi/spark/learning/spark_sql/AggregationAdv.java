package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
public class AggregationAdv implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        Dataset<Row> students = connectors.getSparkSession().read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv");

        students.groupBy("subject")
                .agg(max(col("score")).alias("max_score"), //agg method will automatically casting the value to Integer.
                        min(col("score")).alias("min_score")
                ).show(100);


        students.groupBy("subject")
                .pivot("year")
                .agg(avg(col("score")).alias("average"), //agg method will automatically casting the value to Integer.
                        round(stddev(col("score")), 2).alias("stddev")
                ).show(100);
    }
}
