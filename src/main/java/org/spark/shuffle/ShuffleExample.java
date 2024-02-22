package org.spark.shuffle;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ShuffleExample {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder()
                .appName("Shuffles !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());

    }
}
