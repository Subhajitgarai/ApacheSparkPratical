package org.spark.broadcast;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadCastExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("BroadCast Variables !!")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        Map<String, String> tickerStockName = new HashMap<>() {{
            put("APPL", "Apple Inc");
            put("META", "Meta Platform Inc");
            put("TSLA", "Tesla Inc");
            put("GOOGLE", "Alphabet Inc");
        }};
        Map<String, Double> tickerLastColsePrice = new HashMap<>() {{
            put("GOOGLE", 200.5D);
            put("APPL", 100.1D);
            put("META", 300.3D);
            put("TSLA", 180.5D);
        }};
        Broadcast<Map<String, String>> broadcastTickerStockName = sparkContext.broadcast(tickerStockName);
        Broadcast<Map<String, Double>> broadcastTickerLastClosePrice = sparkContext.broadcast(tickerLastColsePrice);
        try {
            List<String> tickers = List.of("APPL", "META", "TSLA", "GOOGLE");
            JavaRDD<String> myRdd = sparkContext.parallelize(tickers);
            JavaRDD<String> lastClosedPriceRdd = myRdd.map(ticker -> {
                String tickerFullName = broadcastTickerStockName.value().get(ticker);
                Double tickerClosePrice = broadcastTickerLastClosePrice.value().get(ticker);
                return String.format("Ticker= %s, Full Stock Name= %s, Last closed Price=%.2f", ticker, tickerFullName, tickerClosePrice);

            });
            lastClosedPriceRdd.collect().forEach(System.out::println);

        } finally {
            broadcastTickerStockName.destroy(true);
            broadcastTickerLastClosePrice.destroy(true);
        }

    }
}
