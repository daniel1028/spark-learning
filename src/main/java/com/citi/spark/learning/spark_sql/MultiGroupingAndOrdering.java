package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

public class MultiGroupingAndOrdering implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\biglog.txt")
                .createOrReplaceTempView("logging");

        Dataset<Row> logCount = sparkSessionConnector.sql("select level, date_format(datetime,'MMM') as month , count(1) as total from logging group by level, month");
        //logCount.show(10);

        logCount.createOrReplaceTempView("results");
        Dataset<Row> results = sparkSessionConnector.sql("select sum(total) from results");
        results.show();

        Dataset<Row> orderedLog = sparkSessionConnector.sql("select level, date_format(datetime,'MMM') as month,date_format(datetime,'M') as monthNum , count(1) as total from logging group by level, month, monthNum order by monthNum");

        orderedLog.show();
    }
}
