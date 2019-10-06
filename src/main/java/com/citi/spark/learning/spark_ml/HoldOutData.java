package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HoldOutData implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        {
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
            Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[]{0.8, 0.2});
            Dataset<Row> trainingAndTestData = dataSplits[0];
            Dataset<Row> houseHoldData = dataSplits[1];
            LinearRegression linearRegression = new LinearRegression();
            ParamMap[] paramMaps = new ParamGridBuilder()
                    .addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
                    .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.5, 1})
                    .build();

            TrainValidationSplit trainValidationSplit = new TrainValidationSplit();
            trainValidationSplit.setEstimator(linearRegression)
                    .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                    .setEstimatorParamMaps(paramMaps)
                    .setTrainRatio(0.8);

            TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(trainingAndTestData);
            LinearRegressionModel lrModel = (LinearRegressionModel) trainValidationSplitModel.bestModel();

            System.out.print("Trainign Data - R2:"+ lrModel.summary().r2() + " and RMSE:" +lrModel.summary().rootMeanSquaredError() );
            System.out.print("Trainign Data - R2:"+ lrModel.evaluate(houseHoldData).r2() + " and RMSE:" +lrModel.evaluate(houseHoldData).rootMeanSquaredError() );

            System.out.println("Coefficient : " + lrModel.coefficients() + "  intercept:" + lrModel.intercept());
            System.out.println("reg param : " + lrModel.regParam() + "  elastic net param:" + lrModel.elasticNetParam());


        }
    }
}
