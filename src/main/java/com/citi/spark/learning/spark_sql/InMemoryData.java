package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.connectors.SparkContextConnector;
import com.citi.spark.learning.connectors.SparkSessionConnector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

public class InMemoryData implements SparkSessionConnector {
    @Override
    public void execute(SparkSession sparkSession) {

        List<Row> inMemory = new ArrayList<>();
        Row row1 = RowFactory.create("WARN", "12/09/2019");
        inMemory.add(row1);
        inMemory.add(RowFactory.create("WARN", "11/09/2019"));
        inMemory.add(RowFactory.create("INFO", "12/09/2019"));
        inMemory.add(RowFactory.create("DEBUG", "13/09/2019"));
        inMemory.add(RowFactory.create("INFO", "12/20/2019"));
        inMemory.add(RowFactory.create("WARN", "15/012/2019"));


        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dateTime", DataTypes.StringType, true, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> tempLogging = sparkSession.createDataFrame(inMemory, schema);
        tempLogging.show(10);
    }
}
