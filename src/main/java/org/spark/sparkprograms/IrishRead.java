package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class IrishRead {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Irish Dataframe")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        //Reading a Csv file  path
        String path="/home/cbnits-94/IdeaProjects/Iris.csv";
        Dataset<Row> iris=spark.read().csv(path);
        iris.show();
        //Show all the rows
//        iris.show(Integer.MAX_VALUE);
        List<Row>rows=iris.collectAsList();
        rows.forEach(row -> {
            System.out.println(row);
        });

    }



}
