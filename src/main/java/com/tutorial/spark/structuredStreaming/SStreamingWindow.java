package com.tutorial.spark.structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class SStreamingWindow {
    public static void main(String[] args) throws TimeoutException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SStreamingWindow");

        SparkSession session = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = session
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        lines.printSchema();
        System.out.println(lines.isStreaming());

        // Split the lines into words, retaining timestamps
        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");

        String windowDuration = "6 seconds";
        String slideDuration = "2 seconds";

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words.groupBy(
                functions.window(words.col("timestamp"), windowDuration, slideDuration),
                words.col("word")
        ).count().orderBy("window");

        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
