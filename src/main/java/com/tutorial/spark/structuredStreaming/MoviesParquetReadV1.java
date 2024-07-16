package com.tutorial.spark.structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;

import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.desc_nulls_first;

/**
 * <p>Spark Performance</p>
 * <p>
 * data source : <a href="https://github.com/meilisearch/datasets/blob/main/datasets/movies/movies.json">Movie Data</a>
 *
 */
public class MoviesParquetReadV1 {

	/**
	 * <p> Demo1</p>
	 *<img src="../../../../../../../img/orderBy-V0.png"/>
	 *
	 * <p>Solution1</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution.png"/>
	 *
	 * <p>Solution2</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution2.png"/>
	 *
	 * <p>Solution3</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution3-no-shuffle.png"/>
	 *
	 * <p>Solution4</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution4-repartition.png"/>
	 *
	 * <p>Solution5</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution5-sort.png"/>
	 *
	 * <p>Solution6</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution6-coalesce.png"/>
	 *
	 * <p>Solution7</p>
	 * <img src="../../../../../../../img/orderBy-V0-solution7-coalesce-shuffle-part1.png"/>
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
			//.set("spark.sql.shuffle.partitions", "8") // not respected
			.setAppName("MoviesParquetReadV1");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors());
		String inputParquetPath = "parquet/movies_json_to_parquet/movies";

		Dataset<Row> moviesDf = spark.read().parquet(inputParquetPath);
		Utils.printPartitionCount(moviesDf); //4
		//moviesDf.show(); // it only loads 4096 records, not all

		//Dataset<Row> orderedDf = moviesDf.coalesce(2).orderBy(desc_nulls_first("year")); // not sorted
		//Dataset<Row> orderedDf = moviesDf.coalesce(1).sort(desc_nulls_first("year")); // not sorted
		//Dataset<Row> orderedDf = moviesDf.coalesce(1).sortWithinPartitions(desc_nulls_first("year")); // not sorted
		//Dataset<Row> orderedDf = moviesDf.repartition(2).orderBy(desc_nulls_first("year")); // not sorted
		//Dataset<Row> orderedDf = moviesDf.repartition(2).sort(desc_nulls_first("year")); // not sorted
		//Dataset<Row> orderedDf = moviesDf.repartition(1).orderBy(desc("year")); // not sorted
		//spark.conf().set("spark.sql.shuffle.partitions", "2");
		//Dataset<Row> orderedDf = moviesDf.orderBy(desc("year")); // not sorted
		//Dataset<Row> orderedDf = moviesDf.select("year").distinct().orderBy(desc_nulls_first("year"));//sorted

		/*
		Dataset<Row> orderedDf = moviesDf.orderBy(desc_nulls_first("year"));
		// reason :
		// 1. order by not respected
		// 2. shuffle partition = 200
		orderedDf.select("year").distinct().show(150); // not ordered output ==> demo1
		*/

		/*
		spark.conf().set("spark.sql.shuffle.partitions", "1");
		Dataset<Row> orderedDf = moviesDf.orderBy(desc_nulls_first("year"));
		orderedDf.select("year").distinct().show(150); // still not ordered output ==> same as demo1, only partition count = 1
		//orderBy and sort provide same result
		 */

		/*
		spark.conf().set("spark.sql.shuffle.partitions", "1");
		Dataset<Row> orderedDf = moviesDf.sortWithinPartitions(desc_nulls_first("year")); // still not ordered output ==> same as demo1, only partition count = 1
		orderedDf.select("year").distinct().show(150);
		*/
		/*
		//solution1
		Dataset<Row> orderedDf = moviesDf.select("year").distinct().sort(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); // no. of shuffle partitions = 200 //4177
		*/

		/*
		//solution2
		spark.conf().set("spark.sql.shuffle.partitions", "2");
		Dataset<Row> orderedDf = moviesDf.select("year").distinct().sort(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4392
		*/

		/*
		//solution - no image
		spark.conf().set("spark.sql.shuffle.partitions", "2");
		Dataset<Row> orderedDf = moviesDf.select("year").distinct().sortWithinPartitions(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4209
		 */
		/*
		//solution - no image
		spark.conf().set("spark.sql.shuffle.partitions", "1");
		Dataset<Row> orderedDf = moviesDf.select("year").distinct().sortWithinPartitions(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4039 //2nd WINNER
		*/
		/*
		//solution3
		Dataset<Row> orderedDf = moviesDf.coalesce(1).select("year").distinct().sortWithinPartitions(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4026 // no shuffle // single stage //WINNER
		 */
		/*
		//solution4
		Dataset<Row> orderedDf = moviesDf.repartition(1).select("year").distinct().sortWithinPartitions(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4842 // shuffle happened // 4 to 1
		 */
		/*
		//solution5
		Dataset<Row> orderedDf = moviesDf.coalesce(1).select("year").distinct().sort(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4104 // no shuffle // single stage
		 */

		/*
		//solution6
		Dataset<Row> orderedDf = moviesDf.coalesce(2).select("year").distinct().sort(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4345 // shuffle happened // shuffle partition = 200
		 */
		//solution7
		spark.conf().set("spark.sql.shuffle.partitions", "1");
		Dataset<Row> orderedDf = moviesDf.coalesce(2).select("year").distinct().sort(desc_nulls_first("year")); // ordered output
		orderedDf.show(150); //4271 // shuffle happened // shuffle partition = 1
		//After Execution
		Utils.afterExecution(from);

		Utils.waitForInputToClose();


	}

}
