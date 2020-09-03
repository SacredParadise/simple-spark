package com.xl.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SimpleApp2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("192.168.183.68").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

    }



}
