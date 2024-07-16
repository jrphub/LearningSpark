package com.tutorial.spark.structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Objects;
import java.util.Scanner;

import static org.apache.spark.sql.catalyst.util.DateTimeUtils.getYear;

/**
 * <p>Spark Performance</p>
 * <p>
 * data source : <a href="https://github.com/meilisearch/datasets/blob/main/datasets/movies/movies.json">Movie Data</a>
 */
public class MoviesDatasetJsonToParquet {

	public static void main(String[] args) {
		//Before execution
		LocalDateTime from = Utils.getLocalDateTime();

		//Body
		SparkConf conf = new SparkConf()
			.setMaster("local[4]") //Assigning no. of cores in local mode, local[4] means 4 core out of no. of available core
			// if you pass more than no. of available core, then it will ignore that and will continue with default partitioning
			// for my mac with 8 core machine, spark runs with 5 core by default , this means local[10] will result in 5 partition
			//.set("spark.default.parallelism", "4") // Alternatively, to use less than 5 core, but time taken > local[4]
			//.set("spark.sql.shuffle.partitions", "8") // As there is no shuffle involved, hence not used
			.setAppName("MoviesDatasetJsonToParquet");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors()); // 8
		String parquetPath = "parquet/movies_json_to_parquet";

		largeJsonReadAndLoad(spark, parquetPath);

		//After Execution
		Utils.afterExecution(from);

		//Utils.waitForInputToClose();


	}



	/**
	 * @param spark
	 * @param parquetPath <br/>
	 *
	 *                          <a href="https://www.imagebam.com/view/MEUN8WZ" target="_blank"><img src="https://thumbs4.imagebam.com/bd/0e/6e/MEUN8WZ_t.png" alt="spark-read-load-local-mode.png"/></a>
	 */
	private static void largeJsonReadAndLoad(SparkSession spark, String parquetPath) {
		String inputRawPath = "data-raw/movies.json";
		Dataset<Row> moviesDf = spark.read()
			.json(inputRawPath);

		Utils.printPartitionCount(moviesDf);  //depends upon no. of core in local[]

		moviesDf.show(1);
		moviesDf.printSchema();

		/**
		 * root
		 *  |-- genres: array (nullable = true)
		 *  |    |-- element: string (containsNull = true)
		 *  |-- id: long (nullable = true)
		 *  |-- overview: string (nullable = true)
		 *  |-- poster: string (nullable = true)
		 *  |-- release_date: long (nullable = true)
		 *  |-- title: string (nullable = true)
		 */

		//Narrow Transformation Demo
		//Let's create a new column called "year" from the value of "release_date"
		//As the release_date is in long, spark will read it as BIGINT
		//To get the Year from BIGINT, we need to convert it to timestamp first
		// Approach - 1
		//Dataset<Row> moviesDfWyear = moviesDf.withColumn("year", functions.year(functions.from_unixtime(functions.col("release_date"))));
		//Approach - 2
		Dataset<Row> moviesDfWyear = moviesDf.withColumn("year", functions.year(functions.to_timestamp(functions.col("release_date"))));
		Utils.printPartitionCount(moviesDfWyear);
		moviesDfWyear.show(1);
		moviesDfWyear.printSchema();
		/**
		 * root
		 *  |-- genres: array (nullable = true)
		 *  |    |-- element: string (containsNull = true)
		 *  |-- id: long (nullable = true)
		 *  |-- overview: string (nullable = true)
		 *  |-- poster: string (nullable = true)
		 *  |-- release_date: long (nullable = true)
		 *  |-- title: string (nullable = true)
		 *  |-- year: integer (nullable = true)
		 */

		moviesDfWyear.write().mode(SaveMode.Overwrite)
			//.format("noop") //noop works with save(path) , not with parquet
			//.save(String.join("/", parquetPath, "movies"));
			.parquet(String.join("/", parquetPath, "movies"));
	}




}
