package com.tutorial.spark.dataset.sources;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Need to set up mysql server
 */
public class DatasetJDBCMySql {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"DatasetJDBCMySql");

		SparkSession spark = SparkSession.builder()
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "admin");

		String url = "jdbc:mysql://localhost:3306/testdb";

		//create mysql table person
		//CREATE TABLE person ( name varchar(200) NOT NULL, Age int(11) DEFAULT NULL);
		/*insert into person values ('jenny', 15);
		insert into person values ('John', 13);
		insert into person values ('thomas', 11);
		insert into person values ('Robin', 16);
		insert into person values ('Roni', 18);
		insert into person values ('Jacob', 22);
		insert into person values ('Simi', 26);*/

		Dataset<Row> jdbcDFUsers = spark.read().jdbc(url, "person",
				connectionProperties);
		jdbcDFUsers.printSchema();
		/*root
		 |-- name: string (nullable = false)
		 |-- Age: integer (nullable = true)*/

		jdbcDFUsers.show();
		/*+------+---+
		| jenny| 15|
		|  John| 13|
		|thomas| 11|
		| Robin| 16|
		| Jacob| 22|
		|  Roni| 18|
		|  Simi| 26|
		+------+---+*/

		//Moving the person table data to hive
		//spark.sql("CREATE TABLE IF NOT EXISTS person_hive (name STRING, value int)"); //Not Needed
		jdbcDFUsers.write().mode(SaveMode.Overwrite).saveAsTable("person_hive");
		spark.sql("select * from person_hive").show();

		/*+------+---+
		|  name|Age|
		+------+---+
		| jenny| 15|
		|  John| 13|
		|thomas| 11|
		| Robin| 16|
		| Jacob| 22|
		|  Roni| 18|
		|  Simi| 26|
		+------+---+*/

	}

}
