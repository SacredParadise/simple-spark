package com.xl.test.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "/home/xl/test/data/win10.txt"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
//        Dataset<String> logData = spark.read().textFile(logFile).cache();
        JavaRDD<String> fileData = spark.read().textFile(logFile).javaRDD();
        long numAs = fileData.filter(s -> s.contains("a")).count();
        long numBs = fileData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }



}
