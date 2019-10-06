package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CorrelationOfFeature implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {

        Dataset<Row> csvData = sparkSessionConnector.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/inputs/kc_house_data.csv");
        csvData.printSchema();

        csvData.describe().show(10);

       csvData = csvData.drop("id","date","waterfront","view");

        for (String col : csvData.columns()) {
            System.out.println("The correlation between Price and " + col + csvData.stat().corr("price", col));
        }

    }
}
