package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceAnalysis implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> csvData = sparkSessionConnector.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/inputs/kc_house_data.csv");
        csvData.printSchema();

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living"})
                .setOutputCol("features");
        Dataset<Row> modelInputData = vectorAssembler.transform(csvData).select("price", "features")
                .withColumnRenamed("price", "label");
        //Random splitting fo dataset 80% training data and 20% for testdata
        Dataset<Row>[] trainingAndTestData = modelInputData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingData = trainingAndTestData[0];
        Dataset<Row> testData = trainingAndTestData[1];
        LinearRegressionModel model = new LinearRegression().fit(trainingData);
        System.out.print("Trainign Data - R2:"+ model.summary().r2() + " and RMSE:" +model.summary().rootMeanSquaredError() );
        model.transform(testData).show(10);
        System.out.print("Trainign Data - R2:"+ model.evaluate(testData).r2() + " and RMSE:" +model.evaluate(testData).rootMeanSquaredError() );
        //R2 ->     Closer to one is better
        //RMSE -> Smaller is Better




    }
}
