package com.tutorial.spark.dataset.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Objects;

public class DatasetJSONtoParquet {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("DatasetJSONtoParquet");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String inputFilePath = Objects.requireNonNull(DatasetJSONtoParquet.class.getClassLoader().getResource("people.json")).getPath();
        Dataset<Row> peopleDF = spark.read().format("json").load(inputFilePath);

        String outputParquetPath = "output/people.parquet";

        peopleDF.select("name", "age")
                .write().mode(SaveMode.Overwrite)
                .format("parquet")
                .save(outputParquetPath);

        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`output/people.parquet`");
        sqlDF.show();

        /*
        +-------+----+
        |   name| age|
        +-------+----+
        |Michael|null|
        |   Andy|  30|
        | Justin|  19|
        +-------+----+
        */

        peopleDF.createOrReplaceTempView("peopleParquet");
        Dataset<Row> sqlDFTwo = spark.sql("select name from peopleParquet where age between 13 and 19");
        sqlDFTwo.show();
        /*
        +------+
        |  name|
        +------+
        |Justin|
        +------+
         */

        Dataset<String> sqlDS = sqlDFTwo.map(
                (MapFunction<Row, String>) row -> "first name : " + row.getAs("name")
                , Encoders.STRING());

        sqlDS.show();
        /*
        +-------------------+
        |              value|
        +-------------------+
        |first name : Justin|
        +-------------------+
         */

    }

}
