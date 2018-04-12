package org.apache.spark.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/*

    SET GLOBAL log_output = "FILE"; the default.
    SET GLOBAL general_log_file = "/tmp/mysql_queries.log";
    SET GLOBAL general_log = 'ON';

 */
public class MyMlSample {

    static String url = "jdbc:mysql://localhost:3306/testdb?user=root&password=password";

    public static void main(String[] args) throws Exception {
        try {
            SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();
            SQLContext sqlContext = spark.sqlContext();

            Dataset<Row> garbage = sqlContext
                .read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "garbage")
                .load();

            garbage.printSchema();

            StructType name_schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty())});

            //Dataset<Row> names = garbage.map(new Function());

            //Dataset<Row> names = sqlContext.applySchema(garbage, name_schema);

//                // Counts people by age
//            //Dataset<Row> countsByZip = df.groupBy("zip").count();
//            //Dataset<Row> names = people.col("name");
//
//                // Every record of this DataFrame contains the label and
//            // features represented by a vector
//            StructType schema = new StructType(new StructField[]{
//                new StructField("first_name", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("last_name", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("zip", DataTypes.DoubleType, false, Metadata.empty()),
//                new StructField("gender", new VectorUDT(), false, Metadata.empty())});
//            schema.printTreeString();
//
//            Dataset<Row> df = sqlContext
//                .read()
//                .format("jdbc")
//                .option("url", url)
//                .option("dbtable", "garbage")
//                .load();
//
//            //Dataset<Row> df = sqlContext.createDataFrame(data, schema);

            spark.stop();
        } catch (Exception e) {
            System.out.print(e.getMessage());
            throw e;
        }
    }
}
