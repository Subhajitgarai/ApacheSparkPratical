package org.spark.persist;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class PersistExample {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Joining Of pairRdd !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        String path="/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String> myRdd=sparkContext.textFile(path).persist(StorageLevel.MEMORY_AND_DISK());
        System.out.println("Total numbers of lines :="+myRdd.count());
        myRdd.take(5).forEach(System.out::println);
        myRdd.unpersist();
//        System.out.println("Unpersist");
//        JavaRDD<String> unpersist = myRdd.unpersist();
//        unpersist.collect().forEach(System.out::println);
//        System.out.println("UnPersist count="+unpersist.count());
    }
}
