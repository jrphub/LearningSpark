package com.tutorial.spark.structuredStreaming.beans;

import lombok.Data;

@Data
public class Movie {
	private long id;
	private String title;
	private String overview;
	private String[] genres;
	private String poster;
	private long release_date;
}
