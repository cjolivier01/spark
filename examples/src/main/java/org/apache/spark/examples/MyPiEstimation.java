package org.apache.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class MyPiEstimation {

    static int NUM_SAMPLES = 100024;

    /**
     * Main entry point
     * @param args Run arguments
     * @throws Exception In case something goes wrong
     */
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; ++i) {
            l.add(i);
        }

        JavaRDD<Integer> result = jsc.parallelize(l).filter(
                i -> {
                    double x = Math.random();
                    double y = Math.random();
                    return x * x + y * y < 1;  // Boolean, within circle?
                });

        double count = result.count();
        System.out.print("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
        spark.stop();
    }
}
