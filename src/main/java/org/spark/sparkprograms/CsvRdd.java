package org.spark.sparkprograms;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Column$;
import org.apache.spark.sql.SparkSession;

import java.sql.Time;
import java.time.Instant;

public class CsvRdd {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Csv In Rdd")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        String path="/home/cbnits-94/IdeaProjects/Iris.csv";
        JavaRDD<String>myRdd=sparkContext.textFile(path);
        System.out.println("Total numbers of lines :="+myRdd.count());
        myRdd.take(5).forEach(System.out::println);
        System.out.println("-----Only First Line-----");
        System.out.println(myRdd.first());
        System.out.println("-------------------");
        JavaRDD<String[]> csvFeilds = myRdd.map(line -> line.split(","));
        csvFeilds.take(5).forEach(feild->{
            System.out.println(String.join("||",feild[1]));
        });


    }


}
