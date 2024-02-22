package org.spark.transformations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class FlatMapFunctions {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Flat Mapping Test !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        String path="/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String> lines = sc.textFile(path);
        System.out.println("Total Number Of Lines ="+lines.count());
        JavaRDD<List<String>> mapLine = lines.map(line -> List.of(line.split(",")));
        mapLine.take(5).forEach(System.out::println);
        System.out.println("Flat Mapping");
        JavaRDD<String> words = lines.flatMap(line -> List.of(line.split(",")).iterator());
        words.take(10).forEach(System.out::println);
        System.out.println("Total number of Words ="+words.count());


    }
}
