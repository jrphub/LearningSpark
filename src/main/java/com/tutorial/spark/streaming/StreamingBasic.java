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

public class StreamingBasic {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("StreamingBasic");

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
		//apple orange grape apple orange
		JavaDStream<String> words = lines
				.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

		// Count each word in each batch
		// apple 1
		// orange 1
		// grapes 1
		// apple 1
		// orange 1
		JavaPairDStream<String, Long> wordPair = words
				.mapToPair((PairFunction<String, String, Long>) word -> new Tuple2<>(word, 1L));
		
		// apple 2
		// orange 2
		// grapes 1
		JavaPairDStream<String, Long> wordCount = wordPair
				.reduceByKey((Function2<Long, Long, Long>) Long::sum);

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		
		wordCount.print();

		// Start the computation
		jsc.start();

		// Wait for the computation to terminate
		try {
			jsc.awaitTermination();//waits for sigterm ,
			//1. exception in driver - OOM
			//2. exception in executor, delegated to driver
			//3. Manually pass sigterm signal : ctrl+c

			// or jsc.stop() //Programmatically stopping application
			// check for marker file
			//if it is present
			//then jsc.stop(true, true) //1st true : stopping spark context
				//2nd true : stopping application gracefully
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jsc.close();
		}

	}

}
