package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class GroupingAndAggregation implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {

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
        Dataset<Row> tempLogging = connectors.getSparkSession().createDataFrame(inMemory, schema);
        tempLogging.createOrReplaceTempView("logging");

        Dataset<Row> logging = connectors.getSparkSession().sql("select * from logging");
        logging.show(10);

        Dataset<Row> logglevelCount = connectors.getSparkSession().sql("select level, count(1) as occurrences from logging group by level");
        logglevelCount.show(10);

        //This aggregation can crash your servers. so try avoid this aggregation for larger datasets.
        Dataset<Row> dataTimes = connectors.getSparkSession().sql("select level, collect_list(dateTime) as dateTimes from logging group by level");
        dataTimes.show(10);
    }
}
