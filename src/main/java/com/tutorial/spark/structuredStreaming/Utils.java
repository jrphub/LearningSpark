package com.tutorial.spark.structuredStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;

class Utils {

	static void printPartitionCount(Dataset<Row> df) {
		System.out.println("No. of Partitions :" + df.rdd().getNumPartitions());
	}

	static Dataset<Row> printPartitionDist(Dataset<Row> df) {
		return df
			.withColumn("partition", functions.spark_partition_id())
			.groupBy("partition")
			.count()
			.orderBy("partition");
	}

	static @NotNull LocalDateTime getLocalDateTime() {
		LocalDateTime from = LocalDateTime.now();
		System.out.println("Start Time :" + from);
		return from;
	}

	static void waitForInputToClose() {
		Scanner scanner = new Scanner(System.in);
		if (scanner.hasNext()) { //it will wait for input - This will help us to check spark UI and analyse
			scanner.close();
		}
	}

	static void afterExecution(LocalDateTime from) {
		//https://mkyong.com/java8/java-8-difference-between-two-localdate-or-localdatetime/
		LocalDateTime to = LocalDateTime.now();
		System.out.println("End Time :" + to);
		long diff = ChronoUnit.MILLIS.between(from, to);
		System.out.println("Time taken in ms :" + diff);
	}
}
