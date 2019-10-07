package com.citi.spark.learning.jobs;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

@Service
public class Job1 implements SparkJob {

    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        Dataset<Row> logging = connectors.getSparkSession().read().option("header", true).csv("src\\main\\resources\\inputs\\biglog.txt");

        logging.createOrReplaceTempView("logging");

        Dataset<Row> logCount = connectors.getSparkSession().sql("select level, date_format(datetime,'MMM') as month , count(1) as total from logging group by level, month");
        //logCount.show(10);

        logCount.createOrReplaceTempView("results");
        Dataset<Row> results = connectors.getSparkSession().sql("select sum(total) from results");
        results.show();

        Dataset<Row> orderedLog = connectors.getSparkSession().sql("select level, date_format(datetime,'MMM') as month,date_format(datetime,'M') as monthNum , count(1) as total from logging group by level, month, monthNum order by monthNum");

        //Removing column from the dataset. This will create another set of dataset.
        orderedLog.drop("monthNum");

        orderedLog.show(10);

        Dataset<Row> orderedLog2 = connectors.getSparkSession().sql("select level, date_format(datetime,'MMM') as month , count(1) as total from logging group by level,month order by cast(first(date_format(datetime,'M')) as int), level");

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
