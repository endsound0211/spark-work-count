package com.example.spark;


import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

@Slf4j
public class WordCounter {
    public static void main(String[] args){
        var sparkConf = new SparkConf()
                .setAppName("word counter");
        var sc = new JavaSparkContext(sparkConf);

        sc
                .textFile(args[0])
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile(args[1]);
    }
}
