package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FiltersOnSql implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> students = sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv");
        // Dataset<Row> modernArts = students.filter("subject = 'Modern Art'");
        Dataset<Row> modernArts = students.filter("subject = 'Modern Art' AND year >= 2007");
        modernArts.show(10);


        //Using Lambda
        Dataset<Row> modernArtsInLambda = students.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                return (row.getAs("subject").equals("Modern Art") &&
                        ((Number) row.getAs("year")).intValue() >= 2007);
            }
        });
        modernArtsInLambda.show(10);

    }
}
