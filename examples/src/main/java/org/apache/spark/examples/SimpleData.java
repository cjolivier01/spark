package org.apache.spark.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class SimpleData {

    static String url = "jdbc:mysql://localhost:3306/testdb?user=root&password=password";

    public static void main(String[] args) throws Exception {
        try {
            SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();

            //JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            SQLContext sqlContext = spark.sqlContext();

            Dataset<Row> df = sqlContext
                .read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "garbage")
                .load();

            // Look at the schema of this DataFrame
            df.printSchema();

            // Counts people by age
            Dataset<Row> countsByZip = df.groupBy("zip").count();
            countsByZip.show();

            String dirName = "SimpleData_spark_output";
            CJUtil.deleteDirectory(new File(dirName));
            // Saves countsByAge in the JSON format
            countsByZip.write().format("json").save(dirName);

            spark.stop();
        } catch (Exception e) {
            System.out.print(e.getMessage());
            throw e;
        }
    }
}
