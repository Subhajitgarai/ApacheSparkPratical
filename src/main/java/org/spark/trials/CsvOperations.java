package org.spark.trials;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class CsvOperations {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Irish Dataframe")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //Reading a Csv file  path
        String path = "/home/cbnits-94/IdeaProjects/Iris.csv";
        Dataset<Row> df = spark.read().csv(path).toDF();
//        df.show();
        Dataset<Row> selectColumn = df.select("_c0");
        selectColumn.show();
        System.out.println("-----------------------");
        System.out.println("Present Columns");
        String[] columns = df.columns();
        for (String column : columns) {
            System.out.println(column);
        }

//        Dataset<Row> replacedDF = df.withColumn("_c0",
//                functions.when(functions.col("_c0").equalTo("Id"), "first")
//                        .otherwise(functions.col("_c0"))
//        );
//        replacedDF.show();
        //Dropping First Row
        System.out.println("Dropped Row");
        Dataset<Row> filter = df.filter(functions.col("_c0").notEqual("Id"));
        filter.show();
        //Converting strings to Integer
        // Encode the "_c5" column to numerical values
        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("_c5")
                .setOutputCol("label")
                .fit(filter);

        Dataset<Row> indexedDF = indexer.transform(filter);

        // Show DataFrame after encoding
        System.out.println("DataFrame after encoding '_c5' column:");
        indexedDF.show(150);
        System.out.println("--------------------------------------");
        System.out.println("After Dropping c5 column");
        Dataset<Row> dropc5 = indexedDF.drop("_c5");
        dropc5.show();
        System.out.println("Describing");
        Dataset<Row> describe = dropc5.describe();
        describe.show();
        //DataTypes Of Columns
        System.out.println("DataTypes");
        Tuple2<String, String>[] dtypes = dropc5.dtypes();
        for (Tuple2<String, String> dtype : dtypes) {
            System.out.println("Column: " + dtype._1 + ", Data Type: " + dtype._2);
        }


    }
}
