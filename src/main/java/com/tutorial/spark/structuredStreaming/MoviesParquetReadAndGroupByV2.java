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
 * </p>
 *
 * <p>Solution</p>
 * <img src="../../../../../../../img/groupByV0-solution.png"/>
 *
 * <img src="../../../../../../../img/groupByV1-solution.png"/>
 */
public class MoviesParquetReadAndGroupByV2 {

	public static void main(String[] args) {
		//Before execution
		LocalDateTime from = Utils.getLocalDateTime();

		//Body
		SparkConf conf = new SparkConf()
			.setMaster("local[4]") //Assigning no. of cores in local mode, local[4] means 4 core out of no. of available core
			// if you pass more than no. of available core, then it will ignore that and will continue with default partitioning
			// for my mac with 8 core machine, spark runs with 5 core by default , this means local[10] will result in 5 partition
			//.set("spark.default.parallelism", "4") //200 by default// Alternatively, to use less than 5 core, but time taken > local[4]
			//.set("spark.sql.shuffle.partitions", "1") // as there is shuffle involved due to group by below, this value
			// will be respected instead of default value i.e. 200
			//https://stackoverflow.com/questions/45704156/what-is-the-difference-between-spark-sql-shuffle-partitions-and-spark-default-pa
			/**
			 * spark.sql.shuffle.partitions:
			 * Determines how many output partitions you will have after doing wide transformations on Dataframes/Datasets by default.
			 * Its default value is 200.
			 */
			.setAppName("MoviesParquetReadAndGroupByV2");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors());
		String inputParquetPath = "parquet/movies_json_to_parquet/movies";

		Dataset<Row> moviesDf = spark.read().parquet(inputParquetPath);
		System.out.println("Partition Count : " + moviesDf.rdd().getNumPartitions()); //4
		//moviesDf.show(); // it only loads 4096 records, not all
		spark.conf().set("spark.sql.shuffle.partitions", "1");
		Dataset<Row> countForEachYearDs = moviesDf.groupBy("year").count();
		//System.out.println(countForEachYearDs.count()); //114
		countForEachYearDs.show();
		//You can set multiple times for each dataset
		spark.conf().set("spark.sql.shuffle.partitions", "2");
		Dataset<Row> countForEachYearDs2 = moviesDf.groupBy("year").count();
		countForEachYearDs2.show();

		//System.out.println("Partition Count : " + countForEachYearDs.rdd().getNumPartitions()); //1
		//After Execution
		Utils.afterExecution(from);

		Utils.waitForInputToClose();

	}

}
