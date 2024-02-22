package org.spark.sparkprograms;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

//This is for Example
public class RddFromS3 {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("Read Files Form S3 !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext=new JavaSparkContext(sparkSession.sparkContext());
        // Replace Key with AWS account key (can find this on IAM)
        sparkContext.hadoopConfiguration().set("fs.s3a.access.key", "AWS access-key value");

        // Replace Key with AWS secret key (can find this on IAM)
        sparkContext.hadoopConfiguration().set("fs.s3a.secret.key", "AWS secret-key value");

        // Set the AWS S3 end point
        sparkContext.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");

        // Read a single text file
        final org.apache.spark.api.java.JavaRDD<String> myRdd = sparkContext.textFile("s3a://backstreetbrogrammer-bucket/1TrillionWords.txt.gz");
    }
}

