package com.tutorial.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Objects;

public class StreamingTransform {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(
				"StreamingTransform");

		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Batch Interval

		// Receive streaming data from the source
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream(
				"localhost", 9999);

		String stopWordsPath = Objects.requireNonNull(
				StreamingTransform.class.getClassLoader().getResource("stopwords.txt")).getPath();
		JavaRDD<String> stopWordsRDD = jsc
				.sparkContext()
				.textFile(stopWordsPath)
				.map((Function<String, String>) String::toLowerCase);


		// Each record in the "lines" stream is a line of text
		// Split each line into words
		JavaDStream<String> words = lines
				.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.toLowerCase().split(" ")).iterator());

		JavaDStream<String> noStopWordsStream = words
				.transform((Function<JavaRDD<String>, JavaRDD<String>>) rdd -> rdd.subtract(stopWordsRDD));

		noStopWordsStream.print();

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
