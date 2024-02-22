package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
//Fold and Reduce Quite similar to each other the only differenece is to in fold we have to initilze the initoial values
public class FoldFunction {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Rdd Reduce Test!!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        List<Double> data=new ArrayList<>();
        final int dataSize=1_000_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(100* ThreadLocalRandom.current().nextDouble()+47);

        }
        JavaRDD<Double> myRdd = sparkContext.parallelize(data);
        Instant start=Instant.now();
        for (int i = 0; i < 10; i++) {
            Double sum = myRdd.fold(0D,Double::sum);
            System.out.println("SparkRdd Fold Sum="+sum);
        }
        long timeElapsed= Duration.between(start,Instant.now()).toMillis()/10;
        System.out.println("Time taken to Fold Sums avg= "+timeElapsed);


    }
}
