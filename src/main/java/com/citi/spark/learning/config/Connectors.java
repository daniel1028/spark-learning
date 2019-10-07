package com.citi.spark.learning.config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class Connectors {
    Logger LOGGER = Logger.getLogger(Connectors.class);
    private JavaSparkContext context;
    private SparkSession sparkSession;

    @Value("${spline.mode}")
    private String splineMode;

    @Value("${hadoop.home.dir}")
    private String hadoopHome;

    @Value("${spline.mongodb.url}")
    private String mongoDBUrl;

    @Value("${spline.mongodb.name}")
    private String mongoDB;

    @Value("${spark.master.url}")
    private String sparkUrl;

    @Value("${spark.sql.warehouse.dir}")
    private String warehouseLoc;

    @PostConstruct
    private void initSessions() {
        System.setProperty("hadoop.home.dir", hadoopHome);
        System.setProperty("spline.mode", splineMode);
        System.setProperty("spline.persistence.factory", "za.co.absa.spline.persistence.mongo.MongoPersistenceFactory");
        System.setProperty("spline.mongodb.url", mongoDBUrl);
        System.setProperty("spline.mongodb.name", mongoDB);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        LOGGER.info("Initialize java Spark Context");

        SparkConf conf = new SparkConf()
                .setAppName("Spark Learning")
                .setMaster(sparkUrl);
        context = new JavaSparkContext(conf);

        LOGGER.info("Initialize java Spark Session");
        sparkSession = SparkSession.builder().appName("SparkSQL")
                .master(sparkUrl)
                .config("spark.sql.warehouse.dir", warehouseLoc)
                .getOrCreate();

    }

    @PreDestroy
    private void closeSessions() {
        if (context != null) {
            LOGGER.info("Close java Spark Context");
            context.close();
        }

        if (sparkSession != null) {
            LOGGER.info("Close java Spark Session");
            sparkSession.close();
        }
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public JavaSparkContext getSparkContext() {
        return context;
    }
}
