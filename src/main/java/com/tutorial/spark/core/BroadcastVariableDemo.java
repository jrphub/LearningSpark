package com.tutorial.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class BroadcastVariableDemo {

	public static void main(String[] args) throws IOException, URISyntaxException {
		SparkConf sparkConf = new SparkConf().setAppName("Broadcast_Demo")
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
				.getPath()));

		Broadcast<List<String>> bstop = jsc.broadcast(stopWords);

		//To remove stop words from the article
		JavaRDD<String> filteredWords = flat_words
				.filter((Function<String, Boolean>) word -> !bstop.value().contains(word.toLowerCase()));

		filteredWords
				.saveAsTextFile("output/broadcast_" + System.currentTimeMillis());

		jsc.close();

	}

}
