package org.spark.repartitions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Repartition {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Repartition !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        System.out.println("Default Minimum Partitions= " + sparkContext.defaultMinPartitions());
        ArrayList<Integer> data = new ArrayList<>(List.of(1, 2, 3, 4, 5, 6, 7));
        JavaRDD<Integer> myRdd = sparkContext.parallelize(data);
        System.out.println("Initial Partitions= " + myRdd.getNumPartitions());
        JavaRDD<Integer> repartition = myRdd.repartition(2);
        System.out.println("After Repartition Number of Partitions= " + repartition.getNumPartitions());
        sparkSession.stop();



    }
}
