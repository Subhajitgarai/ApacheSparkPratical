package org.spark.transformations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

public class MapFunction {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Mapping Test !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> squaredNumbers = numbers.map(x -> x * x);
        System.out.println(squaredNumbers.collect());
        System.out.println("Total Numbers=" + squaredNumbers.count());
        //Filtering the numbers
        JavaRDD<Integer> filterNum = squaredNumbers.filter(num -> (num < 16));
        filterNum.take(3).forEach(System.out::println);
    }
}
