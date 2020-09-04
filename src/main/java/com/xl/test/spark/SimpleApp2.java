package com.xl.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SimpleApp2 {

    static SparkConf conf = new SparkConf().setMaster("sparl://192.168.183.68").setAppName("My App");
    static JavaSparkContext sc = new JavaSparkContext(conf);


    public static void main(String[] args) {


        JavaRDD<String> input = sc.textFile("s3://...")
        JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) { return Arrays.asList(x.split(" ")); }
        });
        JavaPairRDD<String, Integer> result = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) { return new Tuple2(x, 1); }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer a, Integer b) { return a + b; }
                });


    }

    private static void test2() {
        public static class AvgCount implements Serializable {
            public AvgCount(int total, int num) { total_ = total; num_ = num; }
            public int total_;
            public int num_;
            public float avg() { returntotal_/(float)num_; }
        }
        Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
            public AvgCount call(Integer x) {
                return new AvgCount(x, 1);
            }
        };
        Function2<AvgCount, Integer, AvgCount> addAndCount =
                new Function2<AvgCount, Integer, AvgCount>() {
                    public AvgCount call(AvgCount a, Integer x) {
                        a.total_ += x;
                        a.num_ += 1;
                        return a;
                    }
                };
        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) {
                        a.total_ += b.total_;
                        a.num_ += b.num_;
                        return a;
                    }
                };
        AvgCount initial = new AvgCount(0,0);
        JavaPairRDD<String, AvgCount> avgCounts =
                nums.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }

}
