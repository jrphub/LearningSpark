package com.tutorial.spark.dataset.rddtodataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DatasetCustomized {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("DatasetCustomized");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String inputFilePath = Objects.requireNonNull(DatasetCustomized.class.getClassLoader().getResource("people.csv")).getPath();


        // Create and RDD for datafile
        JavaRDD<String> peopleRDD = spark.read()
                .textFile(inputFilePath)
                .javaRDD();

        // Get the schema at runtime
        // Assuming name and age
        String schemaStr = "name,age";//default : _col0, _col1

        // generate the schema based on string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaStr.split(",")) {
            StructField field = DataTypes.createStructField(fieldName,
                    DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) line -> {
            String[] attributes = line.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDF = spark.createDataFrame(rowRDD, schema);
        System.out.println("people: ");
        peopleDF.show();

        // Creates a temporary view using the DataFrame
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> results = spark
                .sql("select name from people where age between 13 and 19");
        System.out.println("People whose age between 13 and 19");
        results.show();
        /*| name|
        +-----+
        |jenny|
        | John|
        |Robin|
        | Roni|
        | Rimi|
        +-----+*/

        // The results of SQL queries are DataFrames and support all the normal
        // RDD operations
        // The columns of a row in the result can be accessed by field index or
        // by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0)
                , Encoders.STRING());
        namesDS.show();
        /*+-----------+
        |      value|
        +-----------+
        |Name: jenny|
        | Name: John|
        |Name: Robin|
        | Name: Roni|
        | Name: Rimi|
        +-----------+*/

    }

}
