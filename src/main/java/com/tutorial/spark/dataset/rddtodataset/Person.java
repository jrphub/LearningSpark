package com.tutorial.spark.dataset.rddtodataset;

import java.io.Serializable;

public class Person implements Serializable {

	private static final long serialVersionUID = 3240094072676264094L;
	private String name;
	private int age;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

}
