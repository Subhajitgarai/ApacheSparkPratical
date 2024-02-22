package org.spark.joins;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Joining Of pairRdd !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Tuple2<Integer, String>> customers = new ArrayList<>();
        List<Tuple2<Integer, Double>> bills = new ArrayList<>();
        //Adding Customers
        customers.add(new Tuple2<>(1, "John"));
        customers.add(new Tuple2<>(2, "Mary"));
        customers.add(new Tuple2<>(3, "Subhajit"));
        customers.add(new Tuple2<>(4, "Ritwik"));
        customers.add(new Tuple2<>(5, "Sayan"));
        //Adding bills
        bills.add(new Tuple2<>(8, 150.90));
        bills.add(new Tuple2<>(2, 199.90));
        bills.add(new Tuple2<>(9, 2000D));
        bills.add(new Tuple2<>(6, 3000D));
        bills.add(new Tuple2<>(5, 150.90));
        bills.add(new Tuple2<>(10, 250.90));
        bills.add(new Tuple2<>(7, 130.50));
        bills.add(new Tuple2<>(3, 170.50));

        //Creating pairRdd
        JavaPairRDD<Integer, String> customerPair = sparkContext.parallelizePairs(customers);
        JavaPairRDD<Integer, Double> billPair = sparkContext.parallelizePairs(bills);
        //Inner Join Rdd
        System.out.println("Inner Join");
        JavaPairRDD<Integer, Tuple2<String, Double>> innerJoin = customerPair.join(billPair);
        innerJoin.collect().forEach(System.out::println);
        System.out.println("----------------------------");
        //Another way of Accessing the elements
        innerJoin.foreach(content -> {
            System.out.println(content._2._1 + " has order of amount= " + content._2._2);
        });
        System.out.println("-----------------------------");
        //Left Outer Join
        System.out.println("Left Outer Join");
        JavaPairRDD<Integer, Tuple2<String, Optional<Double>>> leftOuterJoin = customerPair.leftOuterJoin(billPair);
        leftOuterJoin.collect().forEach(System.out::println);
        System.out.println("-----------------------------");

        //Another way of Accessing
        leftOuterJoin.foreach(content -> {
            if (content._2._2.isPresent()) {
                System.out.println(content._2._1 + " has order of amount= " + content._2._2.get());
            } else {
                System.out.println(content._2._1 + " has no orders ");
            }
        });
        System.out.println("-----------------------------");
        //Right Outer Join
        System.out.println("Right Outer Join");
        JavaPairRDD<Integer, Tuple2<Optional<String>, Double>> rightOuterJoin = customerPair.rightOuterJoin(billPair);
        rightOuterJoin.collect().forEach(System.out::println);
        System.out.println("--------------------------------");
        //Full outer joins
        System.out.println("Full Outer Joins");
        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Double>>> fullOuterJoin = customerPair.fullOuterJoin(billPair);
        fullOuterJoin.collect().forEach(System.out::println);
        System.out.println("---------------------------------");
        //Cartesian Product
        System.out.println("Cartesian Product");
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Double>> cartesianProduct = customerPair.cartesian(billPair);
        cartesianProduct.collect().forEach(System.out::println);
        System.out.println("---------------------------------");


    }
}







