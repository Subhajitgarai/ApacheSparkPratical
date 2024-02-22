package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FirstCode {
    public static void main(String[] args) {
       try {
           //Creating a SparkSession
           SparkSession spark = SparkSession.builder()
                   .appName("My First Spark Code !!")
                   .master("local[*]")
                   .getOrCreate();
           //Creating SparkContext[EntryPoint]
           JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
           List<Integer>data= Stream.iterate(1, n->n+1).limit(5)
                   .collect(Collectors.toList());
//           Creating Rdd and storeing the data into partitions
          JavaRDD<Integer>myRdd= sc.parallelize(data);
//          JavaRDD<Integer>myRdd= sc.parallelize(data,10); //It will create 10 partitions of data
           System.out.println(myRdd.collect());
           System.out.println("Total Elements in Rdd = "+myRdd.count());
           System.out.println("Default number of partition= "+myRdd.getNumPartitions());
          final int max_ele= myRdd.reduce(Integer::max);
          final int min_ele= myRdd.reduce(Integer::min);
          final int sum= myRdd.reduce(Integer::sum);
           System.out.println("MaxElement= "+max_ele+"\nMinElement= "+min_ele+"\nSum= "+sum);

           //Taking a Testfile by RDD
           String textFilePath="/home/cbnits-94/Desktop/Subhajit/Logging/log.txt";
           JavaRDD<String>logFile=sc.textFile(textFilePath);
           logFile.take(5).forEach(System.out::println);//Top 5 lines Printed
           System.out.println("----------------------\n");
           System.out.println("Total Lines in the File ="+logFile.count());
           //To Print the whole content
//           for (String line : logFile.collect()) {
//               System.out.println(line);
//           }



       }
       catch (Exception e){
           System.out.println(e);
       }
        Scanner sc=new Scanner(System.in);
        System.out.println("Press a to Exit:");
        String p=sc.next();


    }
}
