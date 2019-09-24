package com.citi.spark.learning;

import com.citi.spark.learning.spark_ml.GymCompetitors;
import com.citi.spark.learning.spark_rdd.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class LearningApplication {

    public static void main(String[] args) {
        SpringApplication.run(LearningApplication.class, args);
        LearningApplication.run(args);
    }

    public static void run(String... args) {
        System.setProperty("hadoop.home.dir", "C:/Users/esscay/hadoop-winutils-2.6.0");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext context = createJavaSparkContext();
      /*  new SparkRddBasic().execute(context);
        new SparkRddPracticals().execute(context);
        new ReducesOnRdd().execute(context);
        new MappingOnRdd().execute(context);
        new TuplesOnRdd().execute(context);*/

        //==============================================
        SparkSession sparkSession = createSparkSession();
        //new SparkSQLBasic().execute(sparkSession);
        new GymCompetitors().execute(sparkSession);

        context.close();
        sparkSession.close();
    }

    private static JavaSparkContext createJavaSparkContext() {
        SparkConf conf = new SparkConf()
                .setAppName("Spark Learning")
                .setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
        return context;
    }

    private static SparkSession createSparkSession() {
        return SparkSession.builder().appName("SparkSQL")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();
    }
}
