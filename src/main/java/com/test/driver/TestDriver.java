package com.test.driver;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.tools.nsc.backend.icode.Primitives.Test;

public class TestDriver {

  private final SparkSession sparkSession;

  public TestDriver(){
    this.sparkSession = SparkSession
        .builder()
        .appName("Java Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .config("spark.master", "local")
        .getOrCreate();
  }

  public static void main(String[] args) {
    new TestDriver().run();
  }


  private void run(){
    final JavaRDD<Row> brandRow = spoofRdd(sparkSession);

    /*
     * brand schema
     */
    StructType brandSchema = new StructType(new StructField[]{
        new StructField("n:Name", DataTypes.StringType, false, Metadata.empty()),
        new StructField("n:ExternalId", DataTypes.StringType, false, Metadata.empty())
    });

    final Dataset<Row> brandFrame = sparkSession.createDataFrame(brandRow, brandSchema);

    brandFrame
        .write()
        .format("com.databricks.spark.xml")
        .option("rootTag", "brands")
        .option("rowTag", "brand")
        .mode(SaveMode.Overwrite)
        .save("testfolder");
  }

  private static JavaRDD<Row> spoofRdd(SparkSession sparkSession) {
    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    final JavaRDD<Brand> parallelize = javaSparkContext.parallelize(Arrays
        .asList(Brand.builder().name(".Somename-1").externalId("123").build(),
            Brand.builder().name(".Somename-1").externalId("123").build(),
            Brand.builder().name(".Somename-1").externalId("123").build()));

    return parallelize
        .map((Function<Brand, Row>) v1 -> RowFactory.create(v1.getName(), v1.getExternalId()));
  }

}
