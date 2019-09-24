package com.citi.spark.learning.connectors;

import org.apache.spark.sql.SparkSession;

public interface SparkSessionConnector {
    public void execute (SparkSession sparkSessionConnector);
}
