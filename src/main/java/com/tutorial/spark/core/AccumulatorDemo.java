package com.tutorial.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AccumulatorDemo {

	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("AccumulatorDemo")
				.set("spark.executor.instances", "2").setMaster("local[*]");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> distFile = jsc
				.textFile(requireNonNull(
						BroadcastVariableDemo.class.getClassLoader().getResource("words.txt")).getPath()
				);
		JavaRDD<String> flat_words = distFile
				.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

		List<String> stopWords = Files.readAllLines(
				Paths.get(
						requireNonNull(BroadcastVariableDemo.class.getClassLoader().getResource("stopwords.txt"))
								.getPath())
				);

		LongAccumulator accum = jsc.sc().longAccumulator("stop_words_accumulator");
		
		flat_words.foreach((VoidFunction<String>) word -> {
			if (stopWords.contains(word.toLowerCase())) {
				accum.add(1L);
			}
		});
		
		
		System.out.println("Total no. of stop words present in the article :" + accum.value());

		jsc.close();

	}

}
