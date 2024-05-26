package com.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import org.sparkproject.guava.collect.Iterables;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        List<Double> inputData = new ArrayList<>();
        inputData.add(12.0);
        inputData.add(35.0);
        inputData.add(59.98);
        inputData.add(1.202);

        List<String> inputStringData = new ArrayList<>();
        inputStringData.add("WARN: Tuesday 4 September 0405");
        inputStringData.add("ERROR: Tuesday 4 September 0408");
        inputStringData.add("FATAL: Wednesday 5 September 1632");
        inputStringData.add("ERROR: Friday 7 September 1854");
        inputStringData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<Double> myRdd = sc.parallelize(inputData);
            Double result = myRdd.reduce(Double::sum);
            JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
            sqrtRdd.collect().forEach(System.out::println);
            System.out.println("input sum: " + result);
            System.out.println("count: " + sqrtRdd.count());

            Long count = sqrtRdd.map(value -> 1L).reduce(Long::sum);
            System.out.println("Count: " + count);

            // Tuples
            JavaRDD<Tuple2<Double, Double>> sqrtRdd2 = myRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));

            // Pair RDDs
            JavaRDD<String> originalLogMessages = sc.parallelize(inputStringData);
            JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(msg -> {
                String[] column = msg.split(":");
                String level = column[0];
                String date = column[1];

                return new Tuple2<>(level, 1L);
            });

            JavaPairRDD<String, Long> sumRdd = pairRdd.reduceByKey(Long::sum);
            sumRdd.collect().forEach(val -> System.out.println(val._1 + "\t" + val._2));

            // Fluent API
            sc.parallelize(inputStringData)
                    .mapToPair(msg -> new Tuple2<>(msg.split(":")[0], 1L))
                    .reduceByKey(Long::sum)
                    .collect()
                    .forEach(val -> System.out.println(val._1 + "\t" + val._2));

            // Group By Key
            sc.parallelize(inputStringData)
                    .mapToPair(msg -> new Tuple2<>(msg.split(":")[0], 1L))
                    .groupByKey().collect()
                    .forEach(val -> System.out.println(val._1 + "\t" + Iterables.size(val._2)));

            // Flat Map - to get 0,1 or more output
            JavaRDD<String> words = originalLogMessages.flatMap(msg -> Arrays.asList(msg.split(" ")).iterator());
            JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
            filteredWords.collect().forEach(System.out::println);

            // Reading from Disk
            readFromDisk(sc);
        }

    }

    private static void readFromDisk(JavaSparkContext sc) {
        System.out.println("Reading from Disk");
        JavaRDD<String> initialRdd = sc.textFile("/Users/kargil/Desktop/Spring/Spark-Project/src/main/resources/subtitles/input.txt");
        JavaRDD<String> lettersRdd = initialRdd.map(sentence  -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersRdd.filter(sentence -> !sentence.isBlank());
        JavaRDD<String> words = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> interestingWords = words.filter(word -> word.length() > 3);
        JavaPairRDD<String, Long> pairRdd = interestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
        JavaPairRDD<String, Long> reducedPairRdd = pairRdd.reduceByKey(Long::sum);
        JavaPairRDD<String, Long> sortedPairRdd = reducedPairRdd.sortByKey();
        JavaPairRDD<Long, String> switchedPairRdd = sortedPairRdd.mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).sortByKey(false);
        List<Tuple2<Long, String>> result = switchedPairRdd.take(50);

        result.forEach(System.out::println);
    }
}