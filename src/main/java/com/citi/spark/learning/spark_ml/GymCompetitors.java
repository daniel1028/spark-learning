package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> csvData = sparkSessionConnector.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src\\main\\resources\\inputs\\GymCompetition.csv");
        csvData.printSchema();
        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[]{"Age", "Height", "Weight"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> modelInput = vectorAssembler.transform(csvData);
        modelInput.show(10);

        Dataset<Row> modelDetail = modelInput.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
        modelDetail.show(10);


        csvData.show(10);
    }
}
