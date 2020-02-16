package com.tutorial.spark.dataset.rddtodataset;

import com.tutorial.spark.dataset.encoder.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

public class DatasetUsingReflection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("DatasetUsingReflection");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String inputFilePath = Objects.requireNonNull(DatasetUsingReflection.class.getClassLoader().getResource("people.csv")).getPath();

        System.out.println("Reading csv file and creating dataset (people): ");
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(inputFilePath)
                .javaRDD().map((Function<String, Person>) line -> {
                    String[] cols = line.split(",");

                    Person person = new Person();
                    person.setName(cols[0]);
                    person.setAge(Integer.parseInt(cols[1]));
                    return person;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

        peopleDF.createOrReplaceTempView("people");
        peopleDF.show();

        System.out.println("Finding people whose age between 13 and 19: ");
        Dataset<Row> teenagersDF = spark
                .sql("select name from people where age between 13 and 19");

        teenagersDF.show();

        // want to add 2 years to each people's age
        // So, need to access each field
        // 1. by field index
        System.out.println("Accessing age field by index: age+2");
        Dataset<Integer> agesByIndex = peopleDF
                .map((MapFunction<Row, Integer>) row -> {
                    // lexicographically - age, name
                    return (int) row.get(0) + 2;
                }, Encoders.INT());

        agesByIndex.show();

        // 2. by column name
        System.out.println("Accessing age field by column name: age+2");
        Dataset<Integer> agesByColName = peopleDF
                .map((MapFunction<Row, Integer>) row -> row.<Integer> getAs("age") + 2,
                        Encoders.INT());

        agesByColName.show();

    }

}
