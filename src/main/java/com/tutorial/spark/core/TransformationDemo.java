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

public class TransformationDemo {

	public static void main(String[] args) throws IOException, URISyntaxException {
		SparkConf sparkConf = new SparkConf().setAppName("TransformationDemo")
				.set("spark.executor.instances", "2").setMaster("local[*]");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//RDD creation
		JavaRDD<String> distFile = jsc
				.textFile(requireNonNull(
						TransformationDemo.class.getClassLoader().getResource("article.txt")).getPath()
				);

		//Flatmap
		JavaRDD<String> flat_words = distFile
				.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
		//System.out.println(flat_words.collect());

		List<String> stopWords = Files.readAllLines(
				Paths.get(
						requireNonNull(TransformationDemo.class.getClassLoader().getResource("stopwords.txt"))
				.getPath()));

		Broadcast<List<String>> bstop = jsc.broadcast(stopWords);

		//To remove stop words from the article
		JavaRDD<String> filteredWords = flat_words
				.filter((Function<String, Boolean>) word -> !bstop.value().contains(word.toLowerCase()));

		/*JavaRDD<Long> mappedWords = filteredWords.map((Function<String, Long>) s -> 1L);
		System.out.println("Mapped Words : "+mappedWords.collect());*/


		filteredWords
				.saveAsTextFile("output/broadcast_" + System.currentTimeMillis());

		jsc.close();

	}

}
