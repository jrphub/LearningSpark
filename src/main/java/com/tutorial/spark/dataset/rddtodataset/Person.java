package com.tutorial.spark.dataset.rddtodataset;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
public class Person implements Serializable {

	@Serial
	private static final long serialVersionUID = 3240094072676264094L;
	private String name;
	private int age;
}
