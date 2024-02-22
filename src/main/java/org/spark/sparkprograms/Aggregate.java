package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Aggregate {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Csv In Rdd")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        List<Double> data=new ArrayList<>();
        final int dataSize=1_000_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(100* ThreadLocalRandom.current().nextDouble()+47);

        }
        JavaRDD<Double> myRdd = sparkContext.parallelize(data);
        System.out.println("Total Number oF Partitions:="+myRdd.getNumPartitions());
        Instant start=Instant.now();
        for (int i = 0; i < 10; i++) {
            Double sum = myRdd.aggregate(0D,Double::sum,Double::sum);
            //Max Sum from all partitions
            Double max_sum = myRdd.aggregate(0D,Double::sum,Double::max);
            //Min Sum from all partitions
            Double min_sum = myRdd.aggregate(0D,Double::sum,Double::min);
            System.out.println("SparkRdd Aggregate Sum="+sum);
            System.out.println("Max Sum from all Partitions="+max_sum);
            System.out.println("Min Sum from all Partitions="+min_sum);
        }
        long timeElapsed= Duration.between(start,Instant.now()).toMillis()/10;
        System.out.println("Time taken to Aggregate Sums avg= "+timeElapsed);
    }
}
