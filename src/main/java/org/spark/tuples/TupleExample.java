package org.spark.tuples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

//Tuple is basically a Key value pair
//like var t=("John",25);
public class TupleExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Flat Mapping Test !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        String path="/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String> myRdd = sc.textFile(path);
        System.out.println("Creating a Tuple");
        System.out.println("Total number of Lines= "+myRdd.count());
        JavaRDD<Tuple2> newTuple = myRdd.map(line -> new Tuple2(line,line.length()));
        System.out.println("-------------------------------");
        System.out.println("Tuple Is");
        newTuple.take(5).forEach(System.out::println);
        System.out.println("Total Tuple count ="+newTuple.count());
    }


}
