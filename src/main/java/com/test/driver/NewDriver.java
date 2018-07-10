package com.test.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;

public class NewDriver {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("Unit Test");
    sparkConf.setMaster("local[2]");

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(javaSparkContext);

    StructType customSchema = new StructType(new StructField[] {
        new StructField("_id", DataTypes.StringType, true, Metadata.empty()),
        new StructField("author", DataTypes.StringType, true, Metadata.empty()),
        new StructField("description", DataTypes.StringType, true, Metadata.empty()),
        new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
        new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
        new StructField("title", DataTypes.StringType, true, Metadata.empty())
    });

    /**
     * Keep https://github.com/databricks/spark-xml/blob/master/src/test/resources/books.xml in hdefs fs root location.
     */
    final Dataset<Row> load = sqlContext.read()
        .format("com.databricks.spark.xml")
        .option("rowTag", "book")
        .schema(customSchema)
        .load("books.xml");

    load.select("author", "_id").write()
        .format("com.databricks.spark.xml")
        .mode(SaveMode.Overwrite)
        .option("rootTag", "books")
        .option("rowTag", "book")
        .save("newbooks.xml");
  }

}
