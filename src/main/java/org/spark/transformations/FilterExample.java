package org.spark.transformations;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class FilterExample{
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Filter Transformations!!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        String path="/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String> lines = sc.textFile(path);
        System.out.println("Flat Mapping");
        JavaRDD<String> words = lines.flatMap(line -> List.of(line.split(",")).iterator());
        words.take(10).forEach(System.out::println);
        System.out.println("Total number of Words ="+words.count());
        System.out.println("Filter Functions-");
        JavaRDD<String> filterwords = words.filter(word -> (word.equals("Iris-setosa")));
        filterwords.take(10).forEach(System.out::println);
    }

}
