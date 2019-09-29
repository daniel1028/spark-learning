package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class MultiGroupingAndOrdering implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> logging = sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\biglog.txt");

        logging.createOrReplaceTempView("logging");

        Dataset<Row> logCount = sparkSessionConnector.sql("select level, date_format(datetime,'MMM') as month , count(1) as total from logging group by level, month");
        //logCount.show(10);

        logCount.createOrReplaceTempView("results");
        Dataset<Row> results = sparkSessionConnector.sql("select sum(total) from results");
        results.show();

        Dataset<Row> orderedLog = sparkSessionConnector.sql("select level, date_format(datetime,'MMM') as month,date_format(datetime,'M') as monthNum , count(1) as total from logging group by level, month, monthNum order by monthNum");

        //Removing column from the dataset. This will create another set of dataset.
        orderedLog.drop("monthNum");

        orderedLog.show(10);

        Dataset<Row> orderedLog2 = sparkSessionConnector.sql("select level, date_format(datetime,'MMM') as month , count(1) as total from logging group by level,month order by cast(first(date_format(datetime,'M')) as int), level");

        orderedLog2.show();


        //Using DataFrame - Java API
        Dataset<Row> logs = logging.select(col("level"), date_format(col("datetime"), "MMM").alias("Month")
                , date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType));
        Dataset<Row> grouped = logs.groupBy(col("level"), col("Month"), col("monthNum")).count();
        Dataset<Row> ordered = grouped.orderBy(col("monthNum"), col("level"));
        ordered.drop(col("mothNum"));

        ordered.show(10);

        //Improved way
        logging.select(col("level"), date_format(col("datetime"), "MMM").alias("Month")
                , date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType))
                .groupBy(col("level"), col("Month"), col("monthNum")).count().orderBy(col("monthNum"), col("level"))
                .drop(col("mothNum"))
                .show(10);


    }
}
