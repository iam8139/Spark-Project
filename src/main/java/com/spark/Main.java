package com.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.checkerframework.checker.nullness.Opt;
import org.codehaus.janino.Java;
import org.sparkproject.guava.collect.Iterables;
import scala.Int;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        main(new int[]{1});
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
//            readFromDisk(sc);

            // JavaPairRDD Joins
//            performJoins(sc);

//            Thread.sleep(120 * 1000);
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

    private static void performJoins(JavaSparkContext sc) {
        System.out.println("Performing Inner Joins");

        List<Tuple2<Integer, Integer>> visitors = new ArrayList<>();
        visitors.add(new Tuple2<>(1, 10));
        visitors.add(new Tuple2<>(2, 15));
        visitors.add(new Tuple2<>(3, 8));
        visitors.add(new Tuple2<>(4, 5));

        List<Tuple2<Integer, String>> users = new ArrayList<>();
        users.add(new Tuple2<>(1, "Kargil"));
        users.add(new Tuple2<>(2, "Anurag"));
        users.add(new Tuple2<>(4, "Anand"));
        users.add(new Tuple2<>(5, "Rohit"));
        users.add(new Tuple2<>(6, "Mohit"));

        JavaPairRDD<Integer, Integer> visitorsRdd = sc.parallelizePairs(visitors);
        JavaPairRDD<Integer, String> usersRdd = sc.parallelizePairs(users);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedResult = visitorsRdd.join(usersRdd);

        joinedResult.collect().forEach(System.out::println);

        System.out.println("Performing (Left) Inner Join");

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinResult = visitorsRdd.leftOuterJoin(usersRdd);
        leftJoinResult.collect().forEach(result -> System.out.println(result._2._2.orElse("blank").toUpperCase()));

        System.out.println("Performing (Right) Outer Join");

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> outerJoinResult = visitorsRdd.rightOuterJoin(usersRdd);
        outerJoinResult.collect().forEach(result -> System.out.println(result._2._2 + "\t" + result._2._1.orElse(0)));

        System.out.println("Performing Full Outer Join");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoin = visitorsRdd.fullOuterJoin(usersRdd);
        fullJoin.collect().forEach(result -> System.out.println(result._2._2.orElse("blank").toUpperCase() + "\t" + result._2._1.orElse(0)));

        System.out.println("Cartesian Join");
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoin = visitorsRdd.cartesian(usersRdd);
        cartesianJoin.collect().forEach(result -> System.out.println(result._1 + "\t" + result._2));
    }

    public static void main(int[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession sparkSession = SparkSession.builder().appName("Spark SQL Application").master("local[*]").getOrCreate();

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("/Users/kargil/Desktop/Spring/Delivered Folder/Starting Workspace/Project/src/main/resources/exams/students.csv");
//        dataset.printSchema();
//        dataset.show();

//        System.out.println("There are " + dataset.count() + " records");

        // String Expression filter
        System.out.println("Filter using regular expressions");
        Dataset<Row> filteredResponse = dataset.filter("subject = 'Modern Art' AND year >= '2007' ");
        filteredResponse.show(10);

        // Regular lambda expression filter
        System.out.println("Filter using lambda expressions");
        FilterFunction<Row> fun = row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year")) >= 2007;
        Dataset<Row> filteredResponse2 = dataset.filter(fun);
        filteredResponse2.show(10);

        // Filter using Spark Columns
        System.out.println("Filter using Spark Columns");
        Column subjects = dataset.col("subject");
        Column years = dataset.col("year");
        Dataset<Row> modernArtDataset = dataset.filter(subjects.equalTo("Modern Art").and(years.geq(2007)));
        modernArtDataset.show(10);

        // Filter using functions
        System.out.println("Filter using functions");
        Dataset<Row> colFilter =  dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
        colFilter.show(5);

        // Spark SQL - views
        dataset.createOrReplaceTempView("students");
//        Dataset<Row> sqlResult = sparkSession.sql("select distinct(score) from students where subject='French'");
//        sqlResult.show(10);

        //Spark - In Memory datasets
        System.out.println("In Memory Data - Aggregation and Grouping");

        List<Row> inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] structField = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType structType = new StructType(structField);
        Dataset<Row> dummyDataset = sparkSession.createDataFrame(inMemory, structType);
        dummyDataset.createOrReplaceTempView("log_data");

        Dataset<Row> groupedResult = sparkSession.sql("select level, count(datetime) from log_data group by level order by level");

        groupedResult = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from log_data group by level, month");
        groupedResult.show();

        readCsv(sparkSession, dataset);

//        readTxt(sparkSession);

//        pivotTable(sparkSession);

        sparkSession.close();
    }

    private static void readCsv(SparkSession session, Dataset<Row> dataset) {
        System.out.println("CSV operations using Spark SQL");
        dataset.createOrReplaceTempView("students");

        session.udf().register("passed", score -> score.equals("A+"), DataTypes.BooleanType);
        Dataset<Row> result = dataset.withColumn("pass", callUDF("passed", col("score")));

//        Dataset<Row> result = dataset.groupBy("subject")
//                .pivot("year")
//                .agg(round(avg(col("score")), 2).alias("average"),
//                        round(stddev(col("score")), 2).alias("std"));
//
//        result = dataset.groupBy(col("subject"))
//                .agg(max(col("score")
//                        .cast(DataTypes.IntegerType))
//                        .alias("max score"),
//                        min(col("score")
//                                .cast(DataTypes.IntegerType))
//                                .alias("min score"));
//        result = session.sql("select count(1) as total, subject, first(year) as year from students group by subject, grade order by subject asc, total");
        result.show();
    }

    private static void readTxt(SparkSession session) {
        Dataset<Row> logDataset = session.read().option("header", true).csv("/Users/kargil/Desktop/Spring/Delivered Folder/extras/biglog.txt");
//        Dataset<Row> logData = logDataset.selectExpr("level", "date_format(datetime, 'MMMM') as Month");

        Dataset<Row> logData = logDataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
        logData = logData.groupBy(col("level"), col("month"), col("monthnum"))
                .count()
                .orderBy(first(col("monthnum")), col("level"))
                .drop(col("monthnum"));

        logData.show();
    }

    private static void pivotTable(SparkSession session) {
        Dataset<Row> logDataset = session.read().option("header", true).csv("/Users/kargil/Desktop/Spring/Delivered Folder/extras/biglog.txt");

        Dataset<Row> logData = logDataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        logData = logData.groupBy("level").pivot("month").count();
        logData.show();
    }
}