package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class FiltersOnSql implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> students = sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv");

        //Natural SQL way
        // Dataset<Row> modernArts = students.filter("subject = 'Modern Art'");
        Dataset<Row> modernArts = students.filter("subject = 'Modern Art' AND year >= 2007");
        modernArts.show(10);

        //Using Lambda
        FilterFunction<Row> filterExpr = (row) -> {
            return (row.getAs("subject").equals("Modern Art") &&
                    Integer.parseInt(row.getAs("year")) >= 2007);
        };

        Dataset<Row> modernArtsInLambda = students.filter(filterExpr);
        modernArtsInLambda.show(10);

        //Java Spark SQL way
        Column subjectColumn = students.col("subject");
        Column yearColumn = students.col("year");
        Dataset<Row> modernArtsInJavaSQL = students.filter(subjectColumn.equalTo("Modern Arts")
                .and(yearColumn.geq(2007)));
        modernArtsInJavaSQL.show(100);

        //Optimized way using static functions
        Dataset<Row> modernArtsInJavaSql = students.filter(col("subject").equalTo("Modern Arts")
                .and(col("year").geq(2007)));
        modernArts.show(10);


    }
}
