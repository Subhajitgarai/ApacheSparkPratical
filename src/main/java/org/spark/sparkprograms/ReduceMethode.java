package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ReduceMethode {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Rdd Reduce Test!!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        List<Double>data=new ArrayList<>();
        final int dataSize=1_000_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(100* ThreadLocalRandom.current().nextDouble()+47);

        }
        JavaRDD<Double> myRdd = sparkContext.parallelize(data);
        Instant start=Instant.now();
        for (int i = 0; i < 10; i++) {
            Double sum = myRdd.reduce(Double::sum);
            System.out.println("SparkRdd Reduce Sum="+sum);
        }
        long timeElapsed= Duration.between(start,Instant.now()).toMillis()/10;
        System.out.println("Time taken to Reduce= "+timeElapsed);


    }


}
