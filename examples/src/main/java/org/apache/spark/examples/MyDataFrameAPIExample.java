package org.apache.spark.examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class MyDataFrameAPIExample {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: MyDataFrameAPIExample <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());



        spark.stop();
    }
}
