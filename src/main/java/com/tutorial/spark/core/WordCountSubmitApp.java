package com.tutorial.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Objects;

public class WordCountSubmitApp {
    public static void main(String[] args) {
        String appName="WordCountSubmitApp";
        if (args.length != 2) {
            System.exit(-1);
        }
        String inputFile=args[0];
        String outputDir=args[1];

        SparkConf sparkConf = new SparkConf().setAppName(appName)
                 .set("spark.executor.instances", "2");

        //to run as different user which is a hadoop user, if needed
        System.setProperty("HADOOP_USER_NAME", "huser");

        //if you are connecting with hdfs
		/*
		System.setProperty("HADOOP_CONF_DIR","/home/jrp/softwares/hadoop-2.7.3/etc/hadoop");
		*/

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //1. get input
        JavaRDD<String> distFile = jsc
                .textFile(inputFile);

        //2. Get collection of all words
        //apple orange grapes apple orange
        JavaRDD<String> flat_words = distFile
                .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

        //3.
        // apple 1
        // orange 1
        // grapes 1
        // apple 1
        // orange 1
        JavaPairRDD<String, Long> flat_words_mapped = flat_words
                .mapToPair((PairFunction<String, String, Long>) flat_word -> new Tuple2<>(flat_word, 1L));

        //4.
        // apple 2
        // orange 2
        // grapes 1
        JavaPairRDD<String, Long> flat_words_reduced = flat_words_mapped
                .reduceByKey((Function2<Long, Long, Long>) Long::sum);

        // Delete output Directory if exists
        // hadoop fs -rm -r /user/huser/wordcountHdfs/output

        // All spark job needs file:// or hdfs:// prefix to distinguish between
        // local and cluster
        flat_words_reduced
                .saveAsTextFile(outputDir+"_"+System.currentTimeMillis());

        jsc.close();

    }
}
