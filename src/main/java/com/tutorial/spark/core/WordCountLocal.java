package com.tutorial.spark.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serial;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * <li>For Spark application to run with JDK17, this needs to be added in VM option</li>
 * <code>
 * --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 * </code>
 * <p>Refer : https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct</p>
 *
 * <li>To reflect in all class while running on IDEA, you can add this config in run template as shown below</li>
 * <a href="https://www.imagebam.com/view/MEUJ3AO" target="_blank"><img src="https://thumbs4.imagebam.com/96/ba/d2/MEUJ3AO_t.png" alt="modify-run-template-idea.png"/></a>
 */
public class WordCountLocal {
    public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("WordCountLocal")
                .set("spark.executor.instances", "2")
                .setMaster("local[*]");

        //if you are connecting with hdfs
		/*System.setProperty("HADOOP_USER_NAME", "huser");
		System.setProperty("HADOOP_CONF_DIR","/home/jrp/softwares/hadoop-2.7.3/etc/hadoop");
		*/


        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //1. get input
        JavaRDD<String> distFile = jsc
                .textFile(Objects.requireNonNull(WordCountLocal.class.getClassLoader().getResource("words.txt")).getPath());

        //2. Get collection of all words
        //apple orange grapes apple orange
        JavaRDD<String> flat_words = distFile
                .flatMap(new FlatMapFunction<String, String>() {
                    @Serial
					private static final long serialVersionUID = -7303808940135307768L;

                    public Iterator<String> call(String line) {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });

        //3.
        // apple 1
        // orange 1
        // grapes 1
        // apple 1
        // orange 1
        JavaPairRDD<String, Long> flat_words_mapped = flat_words
                .mapToPair(new PairFunction<String, String, Long>() {
                    @Serial
					private static final long serialVersionUID = 373611082594580542L;

                    public Tuple2<String, Long> call(String flat_word) {
						return new Tuple2<String, Long>(flat_word, 1L);
                    }
                });

        //4.
        // apple 2
        // orange 2
        // grapes 1
        JavaPairRDD<String, Long> flat_words_reduced = flat_words_mapped
                .reduceByKey(new Function2<Long, Long, Long>() {
                    @Serial
					private static final long serialVersionUID = -541165998462813301L;

                    public Long call(Long l1, Long l2) {
                        return l1 + l2;
                    }
                });
        // All spark job needs file:// or hdfs:// prefix to distinguish between
        // local and cluster
        flat_words_reduced
                .saveAsTextFile("output/WordCountLocal_" + System.currentTimeMillis());


        jsc.close();


    }
}
