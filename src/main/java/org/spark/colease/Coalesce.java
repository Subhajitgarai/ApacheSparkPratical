package org.spark.colease;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class Coalesce{
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Repartition !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        System.out.println("Default Minimum Partitions= "+sparkContext.defaultMinPartitions());
        ArrayList<Integer> data=new ArrayList<>();
        data.addAll(List.of(1,2,3,4,5,6,7));
        JavaRDD<Integer> myRdd = sparkContext.parallelize(data);
        System.out.println("Initial Partitions= "+myRdd.getNumPartitions());
        JavaRDD<Integer> coalesce = myRdd.coalesce(3);
        System.out.println("After coalese number of partitions is="+coalesce.getNumPartitions());


    }

}
