package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Udfs implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {
        Dataset<Row> students = sparkSessionConnector.read().option("header", true).csv("src\\main\\resources\\inputs\\students.csv");

        students.withColumn("pass", lit(col("grade").equalTo("A+"))).show(10); //lit -> literal

        //Using UDF Register in spark session
        sparkSessionConnector.udf().register("hasPassed", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);
        students.withColumn("pass", callUDF("hasPassed", col("grade"))).show(10);

        //Udf with more fields
        sparkSessionConnector.udf().register("passResult", (String grade, String subject) -> {
            if (subject.equalsIgnoreCase("Biology")) {
                if (grade.startsWith("A")) {
                    return true;
                }
                return false;
            }
            return grade.startsWith("A") || grade.startsWith("B");
        }, DataTypes.BooleanType);

        students.withColumn("pass",
                callUDF("passResult", col("grade"), col("subject"))).
                show(10);

        //This is something other way to register UDF. Recommended register with lambda.
        sparkSessionConnector.udf().register("passRs", hasPassedFn, DataTypes.BooleanType);
    }

    private static UDF2<String, String, Boolean> hasPassedFn = new UDF2<String, String, Boolean>() {
        @Override
        public Boolean call(String grade, String subject) throws Exception {
            if (subject.equalsIgnoreCase("Biology")) {
                if (grade.startsWith("A")) {
                    return true;
                }
                return false;
            }
            return grade.startsWith("A") || grade.startsWith("B");
        }
    };
}
