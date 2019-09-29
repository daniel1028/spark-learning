package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.SparkSession;

public class HashAggregation implements SparkSessionConnector {
    //Hash aggregation faster than sort aggregation
    //Hash aggregation is only possible if data is MUTABLE
    @Override
    public void execute(SparkSession sparkSessionConnector) {

    }
}
