package com.tutorial.spark.structuredStreaming;

import com.tutorial.spark.structuredStreaming.beans.Movie;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.time.LocalDateTime;

/**
 * <p>Spark Performance</p>
 * <p>
 * data source : <a href="https://github.com/meilisearch/datasets/blob/main/datasets/movies/movies.json">Movie Data</a>
 */
public class MoviesDatasetJsonCustomBean {

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
			.setAppName("MoviesDatasetJsonCustomBean");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors()); // 8

		largeJsonReadAndLoad(spark);

		//After Execution
		Utils.afterExecution(from);

		//Utils.waitForInputToClose();


	}

	private static void largeJsonReadAndLoad(SparkSession spark) {
		String inputRawPath = "data-raw/movies.json";

		Dataset<Movie> moviesDs = spark.read()
			.json(inputRawPath)
			.as(Encoders.bean(Movie.class));

		System.out.println("No. of Partitions :" + moviesDs.rdd().getNumPartitions()); //4

		moviesDs.show(1);
		/**
		 * +--------------------+---+--------------------+--------------------+------------+-----+
		 * |              genres| id|            overview|              poster|release_date|title|
		 * +--------------------+---+--------------------+--------------------+------------+-----+
		 * |[Drama, Crime, Co...|  2|Taisto Kasurinen ...|https://image.tmd...|   593395200|Ariel|
		 * +--------------------+---+--------------------+--------------------+------------+-----+
		 */
		moviesDs.printSchema();


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

		//Let's add a column called year, derived from release_date
		Dataset<Movie> moviesDsWyear = moviesDs
			.withColumn("year", functions.year(functions.to_timestamp(functions.col("release_date"))))
			.as(Encoders.bean(Movie.class));

		moviesDsWyear.show(1);
		/**
		 * +--------------------+---+--------------------+--------------------+------------+-----+----+
		 * |              genres| id|            overview|              poster|release_date|title|year|
		 * +--------------------+---+--------------------+--------------------+------------+-----+----+
		 * |[Drama, Crime, Co...|  2|Taisto Kasurinen ...|https://image.tmd...|   593395200|Ariel|1988|
		 * +--------------------+---+--------------------+--------------------+------------+-----+----+
		 */

		moviesDsWyear.printSchema();
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


	}


}
