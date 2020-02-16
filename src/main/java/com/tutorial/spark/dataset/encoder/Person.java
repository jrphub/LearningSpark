package com.tutorial.spark.dataset.encoder;

import java.io.Serializable;

public class Person implements Serializable {
	private static final long serialVersionUID = 7907691842801302690L;

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
