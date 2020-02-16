package com.tutorial.spark.dataset.mergeschema;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class DatasetSchemaMerging {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("DatasetSchemaMerging");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		List<Square> squares = new ArrayList<Square>();
		for (int i = 0; i <= 5; i++) {
			Square square = new Square();
			square.setValue(i);
			square.setSquare(i * i);
			squares.add(square);
		}

		String outputParquetPath = "output/test_merge";
		// Create a simple DataFrame, store into a partition directory
		Dataset<Row> squareDf = spark.createDataFrame(squares, Square.class);
		System.out.println("Square Dataframe: ");
		squareDf.show();
		squareDf.write().mode(SaveMode.Overwrite).parquet(
				outputParquetPath+"/key=1");
		
		List<Cube> cubes = new ArrayList<Cube>();
		for (int i=6; i<=10; i++) {
			Cube cube = new Cube();
			cube.setCube(i);
			cube.setValue(i * i * i);
			cubes.add(cube);
		}
		
		// Create another DataFrame in a new partition directory,
		// adding a new column and dropping an existing column
		Dataset<Row> cubesDf = spark.createDataFrame(cubes, Cube.class);
		System.out.println("Cube Dataframe");
		cubesDf.show();
		cubesDf.write().mode(SaveMode.Overwrite).parquet(outputParquetPath+"/key=2");
		
		// Read the partitioned table
		Dataset<Row> mergedDF = spark
				.read()
				.option("mergeSchema", true)
				.parquet(
						outputParquetPath)
				.orderBy("key");
		System.out.println("Merged Schema:");
		mergedDF.show();
		/*+------+-----+----+---+
		|square|value|cube|key|
		+------+-----+----+---+
		|     0|    0|null|  1|
		|     1|    1|null|  1|
		|     4|    2|null|  1|
		|    16|    4|null|  1|
		|    25|    5|null|  1|
		|     9|    3|null|  1|
		|  null|  729|   9|  2|
		|  null|  343|   7|  2|
		|  null| 1000|  10|  2|
		|  null|  216|   6|  2|
		|  null|  512|   8|  2|
		+------+-----+----+---+*/
		System.out.println("Printing Schema:");
		mergedDF.printSchema();
		/*root
		 |-- square: integer (nullable = true)
		 |-- value: integer (nullable = true)
		 |-- cube: integer (nullable = true)
		 |-- key: integer (nullable = true)*/
		

	}

}
