package com.tutorial.spark.structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;

/**
 * <p>Spark Performance</p>
 * <p>
 * data source : <a href="https://github.com/meilisearch/datasets/blob/main/datasets/movies/movies.json">Movie Data</a>
 *
 */
public class MoviesParquetReadV0 {

	public static void main(String[] args) {
		//Before execution
		LocalDateTime from = Utils.getLocalDateTime();

		//Body
		SparkConf conf = new SparkConf()
			.setMaster("local[4]") //Assigning no. of cores in local mode, local[4] means 4 core out of no. of available core
			// if you pass more than no. of available core, then it will ignore that and will continue with default partitioning
			// for my mac with 8 core machine, spark runs with 5 core by default , this means local[10] will result in 5 partition
			//.set("spark.default.parallelism", "4") //200 by default// Alternatively, to use less than 5 core, but time taken > local[4]
			//.set("spark.sql.shuffle.partitions", "8") // not respected
			.setAppName("MoviesParquetReadV0");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors());
		String inputParquetPath = "parquet/movies_json_to_parquet/movies";

		Dataset<Row> moviesDf = spark.read().parquet(inputParquetPath);
		Utils.printPartitionCount(moviesDf); //4
		moviesDf.show(); // it only loads 4096 records, not all
		//moviesDf.show(5000); // it only loads next batch of records i.e. total 8192
		System.out.println("No. of rows : " + moviesDf.count()); //31970 // this loads all records, 4 tasks , all on driver,
		// each gives 1 output and at the end it combines


		//After Execution
		Utils.afterExecution(from);

		Utils.waitForInputToClose();


	}

}
