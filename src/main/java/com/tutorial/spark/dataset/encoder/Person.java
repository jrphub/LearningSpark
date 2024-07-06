package com.tutorial.spark.dataset.encoder;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigInteger;

@Data
public class Person implements Serializable {
	@Serial
	private static final long serialVersionUID = 7907691842801302690L;

	private String name;
	private Long age; //instead of int, use BigInteger or Long as while reading json file, jackson treats int value as Long

}
