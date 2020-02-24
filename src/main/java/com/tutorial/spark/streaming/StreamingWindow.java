package com.tutorial.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class StreamingWindow {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"StreamingWindow");

		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Batch Interval

		// Create a DStream that will connect to hostname:port, like
		// localhost:9999
		// This DStream represents streaming data from a TCP source
		// start at terminal : nc -lk 9999
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream(
				"localhost", 9999);

		// Each record in the "lines" stream is a line of text
		// Split each line into words
		JavaDStream<String> words = lines
				.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

		// Count each word in each batch
		JavaPairDStream<String, Long> wordPair = words
				.mapToPair((PairFunction<String, String, Long>) word -> new Tuple2<>(word, 1L));

		Function2<Long, Long, Long> reduceFunc = (Function2<Long, Long, Long>) Long::sum;

		// Reduce last 60 seconds of data, every 30 seconds
		JavaPairDStream<String, Long> windowedWordCounts = wordPair
				.reduceByKeyAndWindow(reduceFunc, Durations.seconds(60),
						Durations.seconds(30));

		windowedWordCounts.print();

		// Start the computation
		jsc.start();

		// Wait for the computation to terminate
		try {
			jsc.awaitTermination();
		} finally {
			jsc.close();
		}
	}
}
