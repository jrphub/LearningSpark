package com.tutorial.spark.structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SkewedDatasetJoinSalting {
	public static void main(String[] args) {
		//Before execution
		LocalDateTime from = Utils.getLocalDateTime();

		//Body
		SparkConf conf = new SparkConf()
			.setMaster("local[*]") //Assigning no. of cores in local mode, local[4] means 4 core out of no. of available core
			// if you pass more than no. of available core, then it will ignore that and will continue with default partitioning
			// for my mac with 8 core machine, spark runs with 5 core by default , this means local[10] will result in 5 partition
			//.set("spark.default.parallelism", "4") // Alternatively, to use less than 5 core, but time taken > local[4]
			//.set("spark.sql.shuffle.partitions", "8") // As there is no shuffle involved, hence not used
			.setAppName("MoviesDatasetJsonToParquet");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors()); // 8

		spark.conf().set("spark.sql.shuffle.partitions", "4");
		spark.conf().set("spark.sql.adaptive.enabled", "false");

		//creating a dataset of numbers
		List<Integer> intList = IntStream.range(1, 1000000).boxed().collect(Collectors.toList());
		Dataset<Integer> oneMillionDs = spark.createDataset(intList, Encoders.INT());
		System.out.println("No. of partitions : " + oneMillionDs.rdd().getNumPartitions()); // doesn't start any job etc.
		Dataset<Row> partitionedDs = oneMillionDs
			.withColumn("partition", functions.spark_partition_id()); //Though the dataset has 8 partition, the functions.spark_partition_id() returns 0 for every row, why???
		//Hence incomplete
		partitionedDs.filter(functions.col("partition").gt(0)).show();
		/*partitionedDs
			.groupBy("partition")
			.count()
			.orderBy(functions.desc("partition"))
			.show(15, false);*/

		//After Execution
		Utils.afterExecution(from);

		Utils.waitForInputToClose();


	}
}
