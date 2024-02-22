package org.spark.exercise;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//Taking a text file and getting the unique words counts from there
public class CountUnique {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Csv In Rdd")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        String textFilePath="/home/cbnits-94/Desktop/Subhajit/Logging/log.txt";
        //Replacing the notations that are not an Englisg Word
        JavaRDD<String>myRdd=sparkContext.textFile(textFilePath).map(line->line.replaceAll("[^a-zA-Z\\s]","").toLowerCase());
//        myRdd.take(10).forEach(System.out::println);
        JavaRDD<String> afterSplit = myRdd.flatMap(line -> List.of(line.split("\\s")).iterator());
        System.out.println("Total word count="+afterSplit.count());
//        afterSplit.take(Integer.MAX_VALUE).forEach(System.out::println);
//        For Distinct words
//        long count = afterSplit.filter(word-> !word.isEmpty()).distinct().count();
//        System.out.println("Distinct word Count="+count);

        JavaPairRDD<String, Long> pairRdd = afterSplit.mapToPair(word -> new Tuple2<>(word, 1L));
//        pairRdd.take(5).forEach(System.out::println);
        JavaPairRDD<String, Long> pairRddReduce = pairRdd.reduceByKey(Long::sum);
//        pairRddReduce.collect().forEach(System.out::println);
        System.out.println("----------------------------------");
        AtomicInteger count = new AtomicInteger(0);
        pairRddReduce.collect().forEach(word -> {
            if (word._2 == 1) {
//                System.out.println(word._1 + " Count = " + word._2);
                count.getAndIncrement();
            }
        });
        System.out.println("Total Unique word count = " + count.get());


    }

}
