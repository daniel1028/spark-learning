package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class PivotTable implements SparkSessionConnector {

    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> logging = sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\biglog.txt");

        logging.select(col("level"), date_format(col("datetime"), "MMM").alias("Month")
                , date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType))
                .groupBy(col("level"))
                .pivot(col("month"))
                .count()
                .show(100);

        //Order column based on monthNum
        logging.select(col("level"), date_format(col("datetime"), "MMM").alias("Month")
                , date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType))
                .groupBy(col("level"))
                .pivot(col("monthNum"))
                .count()
                .show(100);

        //Ordering columns based on name. It will give better performance.
        Object[] months = new Object[]{"January", "Fubrauary", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
        ArrayList<Object> monthHeaders = new ArrayList<>();

        logging.select(col("level"), date_format(col("datetime"), "MMM").alias("Month")
                , date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType))
                .groupBy(col("level"))
                .pivot(col("month"), monthHeaders)
                .count()
                .na().fill(0) //to fill default value if any header columns not available
                .show(100);

    }
}
