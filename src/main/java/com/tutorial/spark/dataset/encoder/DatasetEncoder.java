package com.tutorial.spark.dataset.encoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

public class DatasetEncoder {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("DatasetEncoder");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("Using encoder with custom bean:");
		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32L);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
		  Collections.singletonList(person),
		  personEncoder
		);
		javaBeanDS.show();

		/*
		+---+----+
		|age|name|
		+---+----+
		| 32|Andy|
		+---+----+
		*/

		System.out.println("Using encoder to read json file directly:");
		System.out.println(personEncoder.schema());
		//A usecase : reading json directly and using personEncoder, create dataset of person
		String inputFilePath = Objects.requireNonNull(DatasetEncoder.class.getClassLoader().getResource("people.json")).getPath();
		Dataset<Person> peopleDS = spark.read().json(inputFilePath).as(personEncoder);
		peopleDS.show();

		/*
		+----+-------+
		| age|   name|
		+----+-------+
		|null|Michael|
		|  30|   Andy|
		|  19| Justin|
		+----+-------+
		*/

		// Encoders for most common types are provided in class Encoders
		System.out.println("Using encoder for primitive types :");
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(
				(MapFunction<Integer, Integer>) value -> value + 1
				, integerEncoder);
		transformedDS.show();
		/*
		+-----+
		|value|
		+-----+
		|    2|
		|    3|
		|    4|
		+-----+
		*/

	}

}
