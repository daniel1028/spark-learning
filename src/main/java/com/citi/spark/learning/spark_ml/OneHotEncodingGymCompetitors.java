package com.citi.spark.learning.spark_ml;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OneHotEncodingGymCompetitors implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {
        Dataset<Row> csvData = connectors.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src\\main\\resources\\inputs\\GymCompetition.csv");

        csvData.printSchema();

        StringIndexer stringIndexer = new StringIndexer();
        stringIndexer.setInputCol("Gender");
        stringIndexer.setOutputCol("genderIndex");
        csvData = stringIndexer.fit(csvData).transform(csvData);
        OneHotEncoderEstimator encode = new OneHotEncoderEstimator();
        encode.setInputCols(new String[]{"genderIndex"});
        encode.setOutputCols(new String[]{"gradeVector"});
        csvData = encode.fit(csvData).transform(csvData);

        VectorAssembler vectorAssembler = new VectorAssembler();

        vectorAssembler.setInputCols(new String[]{"Age", "Height", "Weight", "gradeVector"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> modelInput = vectorAssembler.transform(csvData);
        modelInput.show(10);

        Dataset<Row> modelDetail = modelInput.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
        modelDetail.show(10);

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(modelDetail);
        System.out.println("This model has intercepts : " + model.intercept() + " and coefficients : " + model.coefficients());
        model.transform(modelDetail).show(10);

        csvData.show(10);
    }
}
