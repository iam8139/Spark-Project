package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        List<Double> inputData = new ArrayList<>();
        inputData.add(12.0);
        inputData.add(35.0);
        inputData.add(59.98);
        inputData.add(1.202);

        Logger.getLogger("org.apache").setLevel(Level.WARNING);

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
        }
    }
}