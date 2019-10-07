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
public class InMemoryData implements SparkJob {
    @Autowired
    private Connectors connectors;

    @Override
    public void execute() {

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
        Dataset<Row> tempLogging = connectors.getSparkSession().createDataFrame(inMemory, schema);
        tempLogging.show(10);
    }
}
