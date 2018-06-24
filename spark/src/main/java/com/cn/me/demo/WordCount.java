package com.cn.me.demo;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
//		if (args.length < 1) {
//			System.err.println("Usage: JavaWordCount <file>");
//			System.exit(1);
//		}
		if (args.length == 0) {
			String classpath = WordCount.class.getClassLoader().getResource("").getPath();

			String file = classpath + (classpath.endsWith("/") ? "" : "/") + "word.txt" + "";
			args = new String[]{file};
		}
		SparkConf conf = new SparkConf().setAppName("JavaWordCount")
				.setMaster("local") //local
//				.setMaster("spark://xg-spark-master-1.elenet.me:7077")
//				.setMaster("spark://abcdeMacBook-Pro.local:7077")
//				.setMaster("spark://10.104.111.35:8088")
				;
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> lines = sc.textFile(args[0], 1);
		List<String> list = new ArrayList<>();
		for (int i = 0; i < 10 ; i++) {
			list.add(i+"-"+System.currentTimeMillis());
			list.add(1+"-"+System.currentTimeMillis());
		}
		list.add(3+"-"+System.currentTimeMillis());
		JavaRDD<String> lines = sc.parallelize(list);
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				List<String> list = Arrays.asList(SPACE.split(s));
				return list.iterator();
			}

			private static final long serialVersionUID = 1L;

//			@Override
//			public Iterable<String> call(String s) {
//				return Arrays.asList(SPACE.split(s));
//			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		sc.stop();
	}
}
