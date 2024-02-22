package org.spark.pairrdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class ReduceByKey {
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
        pairRDD.take(5).forEach(System.out::println);
        System.out.println("-----------------------------");
        //ReduceByKey
        JavaPairRDD<Integer, Long> sum = pairRDD.reduceByKey(Long::sum);
        sum.take(Integer.MAX_VALUE).forEach(System.out::println);
        System.out.println("------------------------------");
        //Another Way for iterating
        sum.take(10).forEach(line->{
            System.out.println(line._1+"->"+ line._2);
        });

    }
}
