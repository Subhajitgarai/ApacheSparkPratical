package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ReadAllFilesForDirectory {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Irish Dataframe")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //Reading a Csv file  path
        String directoryPath = "/home/cbnits-94/Desktop/Subhajit/Team_work";
        JavaPairRDD<String, String> pairOfFile = sc.wholeTextFiles(directoryPath);
        System.out.println("Total Files in the directory:=" + pairOfFile.count());
        //Print All file names
        pairOfFile.collect().forEach(file -> {
            System.out.println("File_Name=" + file._1);
        });

        //Print all files content
//        pairOfFile.collect().forEach(file->{
//            System.out.println("File_Content\n="+file._2);
//        });
        //Get Name and content at same time
//        pairOfFile.collect().forEach(file->{
//            System.out.println("File_Name="+file._1+"\nContent\n"+file._2);
//        });

//        Printing the contents of Text files only
        pairOfFile.foreach(file -> {
            if (file._1.endsWith("txt")) {
                System.out.println(file._2);
                System.out.println("--------------------");
            }
        });

        //file._1 =>It basically stores the File name with path
        //file._2 =>It Stores the file content
    }
}