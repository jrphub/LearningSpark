package com.tutorial.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StreamingUpdateStateByKey {

    private static JavaStreamingContext createContext(String checkpointDir, String hostname, int port) {
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("StreamingUpdateStateByKey");


        JavaStreamingContext jsc = new JavaStreamingContext(conf,
                Durations.seconds(10)); // Batch Interval

        // set checkpoint directory
        jsc.checkpoint(checkpointDir);

        // Receive streaming data from the source
        JavaReceiverInputDStream<String> lines = jsc
                .socketTextStream(hostname, port);

        // Each record in the "lines" stream is a line of text
        // Split each line into words
        //apple orange grapes apple orange
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

        JavaPairDStream<String, Long> runningCounts = wordPair.updateStateByKey(
                (Function2<List<Long>, Optional<Long>, Optional<Long>>) (values, state) -> {
                    Long currentSum = state.orElse(0L); // state != null ?
                    // state : OL;
                    for (Long value : values) { //1, 1
                        currentSum = currentSum + value;
                    }
                    return Optional.of(currentSum);
                });

        runningCounts.print();

        return jsc;

    }

    public static void main(String[] args) throws InterruptedException {

        String checkpointDirectory = "spark-checkpoint";
        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(checkpointDirectory, "localhost", 9999);

        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);

        // Start the computation
        context.start();

        // Wait for the computation to terminate
        try {
            context.awaitTermination();
        } finally {
            context.close();
        }

    }

}
