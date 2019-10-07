package com.citi.spark.learning.spark_sql;

import com.citi.spark.learning.config.Connectors;
import com.citi.spark.learning.config.SparkJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HashAggregation implements SparkJob {
    @Autowired
    private Connectors connectors;

    //Hash aggregation faster than sort aggregation
    //Hash aggregation is only possible if data is MUTABLE
    @Override
    public void execute() {
        Dataset<Row> logging = connectors.getSparkSession().read().option("header", true).csv("src\\main\\resources\\inputs\\biglog.txt");
        logging.createOrReplaceTempView("logging");

        //Using SortAggregation
        Dataset<Row> orderedLog2 = connectors.getSparkSession().sql("select level, date_format(datetime,'MMM') as month, count(1) as total from logging group by level,month order by cast(first(date_format(datetime,'M')) as int), level");
        orderedLog2.explain(); // This is will give you computation information
        orderedLog2.show(10);

        //Performance improved. HashAggregation

        Dataset<Row> orderedLog3 = connectors.getSparkSession().sql("select level, date_format(datetime,'MMM') as month,first(cast(date_format(datetime,'M') as int )) as monthNum , count(1) as total from logging group by level,month order by monthNum, level");
        orderedLog3.explain();
        orderedLog3.show(10);

        Dataset<Row> orderedLog = connectors.getSparkSession().sql("select level, date_format(datetime,'MMM') as month,date_format(datetime,'M') as monthNum , count(1) as total from logging group by level, month, monthNum order by monthNum");

        orderedLog.show(10);
        orderedLog.explain();
    }
}
