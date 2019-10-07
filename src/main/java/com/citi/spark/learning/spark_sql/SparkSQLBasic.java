package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SparkSQLBasic implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        connectors.getSparkSession().read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv")
                .createOrReplaceTempView("students");

        Dataset<Row> students = connectors.getSparkSession().sql("select * from students");

        System.out.println("Rows Count : " + students.count());
        students.show(100);

        Row firstRow = students.first();
        System.out.println("Subject : " + firstRow.getAs("subject")); //Using header
        System.out.println("Year : " + firstRow.get(3)); //Using index of the header

    }
}
