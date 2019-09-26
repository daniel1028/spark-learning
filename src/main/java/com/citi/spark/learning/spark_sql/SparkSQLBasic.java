package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLBasic implements SparkSessionConnector {

    @Override
    public void execute(SparkSession sparkSession) {
        sparkSession.read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv")
                .createOrReplaceTempView("students");

        Dataset<Row> students = sparkSession.sql("select * from students");

        System.out.println("Rows Count : " + students.count());
        students.show(100);

        Row firstRow = students.first();
        System.out.println("Subject : " + firstRow.getAs("subject")); //Using header
        System.out.println("Year : " + firstRow.get(3)); //Using index of the header

    }
}
