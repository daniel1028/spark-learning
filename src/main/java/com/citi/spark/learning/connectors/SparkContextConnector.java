package com.citi.spark.learning.connectors;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkContextConnector {
    public void execute(JavaSparkContext context);
}
