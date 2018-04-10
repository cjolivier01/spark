package org.apache.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class MyDataFrameAPIExample {

    public static Row do_create(Object ... values) {
        return new GenericRow(values);
    }

    public static void main(String[] args) throws Exception {
        try {
            if (args.length < 1) {
                System.err.println("Usage: MyDataFrameAPIExample <file>");
                System.exit(1);
            }

            SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();

            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            JavaRDD<String> textFile = sc.textFile(args[0]);
            JavaRDD<Row> rowRDD = textFile.map(MyDataFrameAPIExample::do_create);

            List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true)
            );

            StructType schema = DataTypes.createStructType(fields);

            Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

            Dataset<Row> errors = df.filter(new Column("line").like("%check%"));

            // Count all the errors
            long errorCount = errors.count();

            System.out.println("Error count: " + errorCount);

            // Counts errors mentioning MySQL
            errorCount = errors.filter(new Column("line").like("%error%")).count();
            // Fetches the MySQL

            System.out.println("MYSQL Error count: " + errorCount);

            spark.stop();
        } catch (Exception e) {
            System.out.print(e.getMessage());
            throw e;
        }
    }
}


