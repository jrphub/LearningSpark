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
 */
public class MoviesParquetReadAndGroupByV0 {
	/**
	 *
	 * @param args
	 * <br/>
	 * <p>SQL/Dataframe</p>
	 * <img src="../../../../../../../img/groupByV0-dataframe.png"/>
	 *
	 * <p>Plan</p>
	 * Refer img/groupByV0-plan.png
	 *
	 * <p>JobID 1</p>
	 * <img src="../../../../../../../img/groupByV0-jobId1.png"/>
	 *
	 * <p>JobID 2</p>
	 *<img src="../../../../../../../img/groupByV0-jobId2.png"/>
	 *
	 * <p>Problem</p>
	 *<img src="../../../../../../../img/groupByV0-problem.png"/>
	 */
	public static void main(String[] args) {
		//Before execution
		LocalDateTime from = Utils.getLocalDateTime();

		//Body
		SparkConf conf = new SparkConf()
			.setMaster("local[4]") //Assigning no. of cores in local mode, local[4] means 4 core out of no. of available core
			// if you pass more than no. of available core, then it will ignore that and will continue with default partitioning
			// for my mac with 8 core machine, spark runs with 5 core by default , this means local[10] will result in 5 partition
			//.set("spark.default.parallelism", "4") //200 by default// Alternatively, to use less than 5 core, but time taken > local[4]
			.setAppName("MoviesParquetReadAndGroupByV0");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors()); // 8

		String inputParquetPath = "parquet/movies_json_to_parquet/movies";
		Dataset<Row> moviesDf = spark.read().parquet(inputParquetPath);
		Utils.printPartitionCount(moviesDf); //4

		//moviesDf.show(); // it only loads 4096 records, not all

		Dataset<Row> countForEachYearDs = moviesDf.groupBy("year").count();
		//System.out.println(countForEachYearDs.count()); //114
		countForEachYearDs.show();

		Utils.printPartitionCount(countForEachYearDs); //1
		//After Execution
		Utils.afterExecution(from);

		Utils.waitForInputToClose();
	}
}
