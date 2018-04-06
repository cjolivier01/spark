package org.apache.spark.examples;

import org.apache.spark.sql.SparkSession;

public class CJUtil {
    public static SparkSession getSession(String appName) {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master("local[*]")
                .getOrCreate();
        return spark;
    }
}
