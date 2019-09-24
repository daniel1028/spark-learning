package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.SparkSession;

public class SparkSQLBasic implements SparkSessionConnector {

    @Override
    public void execute(SparkSession sparkSession) {
        sparkSession.read().option("header", true).csv("C:\\Users\\esscay\\IdeaProjects\\learn-spark\\src\\main\\resources\\inputs")
                .createOrReplaceTempView("logging");
        sparkSession.sql("select * from logging")
        .show(100);

    }
}