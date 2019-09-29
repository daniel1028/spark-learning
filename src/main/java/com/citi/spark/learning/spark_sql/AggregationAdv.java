package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class AggregationAdv implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> students = sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv");

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
