package com.mjduan.project;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Hans on 2017-06-05 07:04
 */
public class App2 {

    private static final String URL = "jdbc:mysql://localhost:3306/mybank2?useUnicode=true&characterEncoding=UTF-8";

    public static void main(String[] args) {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", "root");//username
        connectionProperties.setProperty("password", "123");// password

        SparkSession sparkSession = SparkSession.builder()
                .appName("java spark mysql")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> rowDataset = sparkSession.read().jdbc(URL, "t_record", connectionProperties);
        rowDataset.select(col("id"),col("ipv4"),col("ipv6"))
                .orderBy(col("time"))
                .limit(5)
                .show();

        sparkSession.close();
    }


}
