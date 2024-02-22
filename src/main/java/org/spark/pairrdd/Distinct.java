package org.spark.pairrdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class Distinct {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Csv In Rdd")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        String path = "/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String> myRdd = sparkContext.textFile(path);
        System.out.println("Total numbers of lines :=" + myRdd.count());
        JavaPairRDD<Integer,Long> pairRDD = myRdd.mapToPair(line -> new Tuple2<>(line.length(), 1L));
        JavaPairRDD<Integer, Long> distinct = pairRDD.distinct();
        distinct.take(10).forEach(System.out::println);
        System.out.println("Total Unique Lines in the File="+distinct.count());
    }
}
