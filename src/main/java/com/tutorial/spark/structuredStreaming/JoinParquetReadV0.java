package com.tutorial.spark.structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;

import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.desc;

/**
 * <p>Spark Performance</p>
 *
 * <p> Join without AQE and best practices</p>
 * <img src="../../../../../../../img/join-without-aqe-basic.png"/>
 *
 * <img src="../../../../../../../img/join-without-aqe-basic-tasks.png"/>
 *
 * <img src="../../../../../../../img/join-without-aqe-basic-plan.png"/>
 *
 */
public class JoinParquetReadV0 {

	public static void main(String[] args) {
		//Before execution
		LocalDateTime from = Utils.getLocalDateTime();

		//Body
		SparkConf conf = new SparkConf()
			.setMaster("local[*]") //Assigning no. of cores in local mode, local[4] means 4 core out of no. of available core
			// if you pass more than no. of available core, then it will ignore that and will continue with default partitioning
			// for my mac with 8 core machine, spark runs with 5 core by default , this means local[10] will result in 5 partition
			//.set("spark.default.parallelism", "4") //200 by default// Alternatively, to use less than 5 core, but time taken > local[4]
			//.set("spark.sql.shuffle.partitions", "8") // not respected until there is no shuffle
			.setAppName("JoinParquetReadV0");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		System.out.println("No. of CPU cores available : " + Runtime.getRuntime().availableProcessors());
		String inputTransPath = "parquet/transactions_parquet";
		String inputCustomerPath = "parquet/customers_parquet";

		Dataset<Row> transactionDf = spark.read().parquet(inputTransPath);
		Utils.printPartitionCount(transactionDf); //4
		//transactionDf.show(5, false); // it only loads 4096 records, not all

		Dataset<Row> customerDf = spark.read().parquet(inputCustomerPath);
		Utils.printPartitionCount(customerDf); //4
		//customerDf.show(5, false); // it only loads 4096 records, not all

		/*transactionDf
			.groupBy("cust_id")
			.agg(countDistinct("txn_id").alias("countOfTransaction"))
				.orderBy(desc("countOfTransaction"))
					.show(5, false);*/

		/**
		 * Skewed Data
		 * +----------+------------------+
		 * |cust_id   |countOfTransaction|
		 * +----------+------------------+
		 * |C0YDPQWPBJ|17539732          |
		 * |CBW3FMEAU7|7999              |
		 * |C3KUDEN3KO|7999              |
		 * |C89FCEGPJP|7999              |
		 * |CHNFNR89ZV|7998              |
		 * +----------+------------------+
		 *
		 */

		spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");
		spark.conf().set("spark.sql.adaptive.enabled", "false");
		spark.conf().set("spark.sql.adaptive.skewJoin.enabled", "false");
		//want to do Sort-merge join

		Dataset<Row> txnDetailsJoin = transactionDf.join(customerDf, "cust_id", "inner");

		txnDetailsJoin.count(); // 13926

		//After Execution
		Utils.afterExecution(from);

		Utils.waitForInputToClose();


	}

}
