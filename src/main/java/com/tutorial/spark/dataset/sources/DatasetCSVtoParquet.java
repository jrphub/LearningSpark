package com.tutorial.spark.dataset.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.immutable.$colon$colon;

import java.util.Objects;

public class DatasetCSVtoParquet {

    public static void main(String[] args) {
        String outputParquetPath = "output/csv_to_parquet";
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("DatasetCSVtoParquet");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String inputFilePath = Objects.requireNonNull(DatasetCSVtoParquet.class.getClassLoader().getResource("peopleWHeader.csv")).getPath();

        System.out.println("Reading csv file and creating dataset (people): ");
        Dataset<Row> peopleDF = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(inputFilePath);
        peopleDF.show();

        peopleDF.write().mode(SaveMode.Overwrite).parquet(outputParquetPath);


    }

}
