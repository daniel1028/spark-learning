package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PipelineStages implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        Dataset<Row> csvData = connectors.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/inputs/kc_house_data.csv");
        csvData.printSchema();

        Dataset<Row>[] dataSplits = csvData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingAndTestData = dataSplits[0];
        Dataset<Row> houseHoldData = dataSplits[1];

        StringIndexer gradeIndexer = new StringIndexer();
        gradeIndexer.setInputCol("grade");
        gradeIndexer.setOutputCol("gradeIndex");
        /*csvData = gradeIndexer.fit(csvData).transform(csvData);*/

        StringIndexer zipcodeIndex = new StringIndexer();
        zipcodeIndex.setInputCol("zipcode");
        zipcodeIndex.setOutputCol("zipcodeIndex");
        /*csvData = zipcodeIndex.fit(csvData).transform(csvData);*/

        OneHotEncoderEstimator encode = new OneHotEncoderEstimator();
        encode.setInputCols(new String[]{"gradeIndex", "zipcodeIndex"});
        encode.setOutputCols(new String[]{"gradeVector", "zipcodeVector"});
        /*csvData = encode.fit(csvData).transform(csvData);*/

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "gradeVector", "zipcodeVector"})
                .setOutputCol("features");
        Dataset<Row> modelInputData = vectorAssembler.transform(csvData).select("price", "features")
                .withColumnRenamed("price", "label");


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


        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{gradeIndexer, zipcodeIndex, encode, vectorAssembler, trainValidationSplit});
        org.apache.spark.ml.PipelineModel pipelineModel = pipeline.fit(trainingAndTestData);
        TrainValidationSplitModel trainValidationSplitModel = (TrainValidationSplitModel) pipelineModel.stages()[5];
        LinearRegressionModel lrModel = (LinearRegressionModel) trainValidationSplitModel.bestModel();

        System.out.print("Trainign Data - R2:" + lrModel.summary().r2() + " and RMSE:" + lrModel.summary().rootMeanSquaredError());
        System.out.print("Trainign Data - R2:" + lrModel.evaluate(houseHoldData).r2() + " and RMSE:" + lrModel.evaluate(houseHoldData).rootMeanSquaredError());

        System.out.println("Coefficient : " + lrModel.coefficients() + "  intercept:" + lrModel.intercept());
        System.out.println("reg param : " + lrModel.regParam() + "  elastic net param:" + lrModel.elasticNetParam());

    }
}
