package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CorrelationOfFeature implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        Dataset<Row> csvData = connectors.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/inputs/kc_house_data.csv");
        csvData.printSchema();

        csvData.describe().show(10);

        csvData = csvData.drop("id", "date", "waterfront", "view");

        for (String col : csvData.columns()) {
            System.out.println("The correlation between Price and " + col + csvData.stat().corr("price", col));
        }

    }
}
