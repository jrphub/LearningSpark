package com.tutorial.spark.streaming;

public class StreamingKafkaDirect {/*

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(
				"Streaming_Kafka_Direct");

		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.minutes(1));


		String brokers = "localhost:9092";
		String topics = "test-spark";

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("group.id", "console-consumer-37748");


		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);
		//topic_name, data
		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String msg) {
						return Arrays.asList(SPACE.split(msg)).iterator();
					}
				});

		JavaPairDStream<String, Integer> wordCounts = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				});

		JavaPairDStream<String, Integer> result = wordCounts
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		wordCounts.print();

		jssc.start();
		try {
			jssc.awaitTermination();
		} finally {
			jssc.close();
		}

	}*/
}
