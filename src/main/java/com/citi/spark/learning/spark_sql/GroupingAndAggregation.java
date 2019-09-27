package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class GroupingAndAggregation implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSessionConnector) {

        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "12/09/2019"));
        inMemory.add(RowFactory.create("DEBUG", "11/09/2019"));
        inMemory.add(RowFactory.create("INFO", "12/09/2019"));
        inMemory.add(RowFactory.create("INFO", "10/10/2019"));
        inMemory.add(RowFactory.create("DEBUG", "01/08/2019"));
        inMemory.add(RowFactory.create("DEBUG", "13/09/2019"));
        inMemory.add(RowFactory.create("INFO", "12/20/2019"));
        inMemory.add(RowFactory.create("WARN", "15/012/2019"));

        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dateTime", DataTypes.StringType, true, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> tempLogging = sparkSessionConnector.createDataFrame(inMemory, schema);
        tempLogging.createOrReplaceTempView("logging");

        Dataset<Row> logging = sparkSessionConnector.sql("select * from logging");
        logging.show(10);

        Dataset<Row> logglevelCount = sparkSessionConnector.sql("select level, count(1) as occurrences from logging group by level");
        logglevelCount.show(10);

        //This aggregation can crash your servers. so try avoid this aggregation for larger datasets.
        Dataset<Row> dataTimes = sparkSessionConnector.sql("select level, collect_list(dateTime) as dateTimes from logging group by level");
        dataTimes.show(10);
    }
}
