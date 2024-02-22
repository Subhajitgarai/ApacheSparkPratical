package org.spark.pairrdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class PairRdds {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Csv In Rdd")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        String path = "/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String> myRdd = sparkContext.textFile(path);
        System.out.println("Total numbers of lines :=" + myRdd.count());
        JavaPairRDD<Integer,String> pairRDD = myRdd.mapToPair(line -> new Tuple2<>(line.length(), line));
        pairRDD.take(5).forEach(System.out::println);
        System.out.println("-----------------------------");


    }
}
