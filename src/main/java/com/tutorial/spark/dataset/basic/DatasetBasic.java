package com.tutorial.spark.dataset.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.List;
import java.util.Objects;

public class DatasetBasic {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("DatasetBasic");
		
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();

		String inputDataPath = Objects.requireNonNull(DatasetBasic.class.getClassLoader().getResource("people.json")).getPath();

		Dataset<Row> df = spark
				.read()
				.json(inputDataPath);
		System.out.println("Printing input dataframe :");
		df.show();
		/*+----+-------+
		| age|   name|
		+----+-------+
		|null|Michael|
		|  30|   Andy|
		|  19| Justin|
		+----+-------+*/

		System.out.println("Printing Schema :");
		df.printSchema();
		/*
		root
		 |-- age: long (nullable = true)
		 |-- name: string (nullable = true)*/

		System.out.println("Only name :");
		df.select("name").show();
		/*+-------+
		|   name|
		+-------+
		|Michael|
		|   Andy|
		| Justin|
		+-------+*/

		System.out.println("Printing (name , age+1)");
		//https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
		df.select(functions.col("name"), functions.col("age").plus(1)).show();
		/*
		+-------+---------+
		|   name|(age + 1)|
		+-------+---------+
		|Michael|     null|
		|   Andy|       61|
		| Justin|       20|
		+-------+---------+*/

		System.out.println("People with age > 21");
		df.filter(functions.col("age").gt(21)).show();
		/*
		+---+----+
		|age|name|
		+---+----+
		| 30|Andy|
		+---+----+
		*/

		System.out.println("Group by age :");
		df.groupBy("age").count().show();
		/*
		+----+-----+
		| age|count|
		+----+-----+
		|  19|    1|
		|null|    1|
		|  30|    1|
		+----+-----+
		*/
		final List<Row> ageList = df.groupBy("age").count().collectAsList();
		for (Row row : ageList) {
			System.out.println(row.get(0) + ":" + row.get(1));
		}
		//Running queries programmatically
		
		/*
		 * The sql function on a SparkSession enables applications to run SQL
		 * queries programmatically and returns the result as a Dataset<Row>.
		 */
		System.out.println("Using Sql :");
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
		
		/*
		+----+-------+
		| age|   name|
		+----+-------+
		|null|Michael|
		|  30|   Andy|
		|  19| Justin|
		+----+-------+
		*/

		//Temporary table is not accessible across session
		//NoSuchTableException: Table or view 'people' not found in database 'default';
		//spark.newSession().sql("SELECT * FROM people").show();
		
		//Global Temporary view
		
		/*
		 * Temporary views in Spark SQL are session-scoped and will disappear if
		 * the session that creates it terminates. If you want to have a
		 * temporary view that is shared among all sessions and keep alive until
		 * the Spark application terminates, you can create a global temporary
		 * view. Global temporary view is tied to a system preserved database
		 * global_temp
		 * 
		 */

		try {
			df.createGlobalTempView("peopleGlobal");
		} catch (Exception e) {
			e.printStackTrace();
		}


		System.out.println("Using global session :");
		spark.sql("SELECT * FROM global_temp.peopleGlobal").show();
		
		/*
		+----+-------+
		| age|   name|
		+----+-------+
		|null|Michael|
		|  30|   Andy|
		|  19| Justin|
		+----+-------+
		*/

		// Global temporary view is cross-session
		System.out.println("cross-session access:");
		spark.newSession().sql("SELECT * FROM global_temp.peopleGlobal").show();
		/*
		+----+-------+
		| age|   name|
		+----+-------+
		|null|Michael|
		|  30|   Andy|
		|  19| Justin|
		+----+-------+
		 */


	}

}
