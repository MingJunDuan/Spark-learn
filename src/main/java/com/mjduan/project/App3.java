package com.mjduan.project;

import java.util.Properties;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Read data from mysql then transform them to JSON and then write the result into mongoDB
 *
 *
 * Hans on 2017-06-05 08:49
 */
public class App3 {

    private static final String URL = "jdbc:mysql://localhost:3306/mybank2?useUnicode=true&characterEncoding=UTF-8";

    public static void main(String[] args) {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", "root");//username
        connectionProperties.setProperty("password", "123");// password

        SparkSession sparkSession = SparkSession.builder()
                .appName("java spark mysql")
                .master("local[4]")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark-mongodb.output")
                .getOrCreate();

        Dataset<Row> rowDataset = sparkSession.read().jdbc(URL, "t_mypay", connectionProperties);
        Dataset<Row> dataset = rowDataset.select(col("id"), col("money"), col("remark"), col("user"), col("time"))
                .orderBy(col("id"));
        //Transform to json
        Dataset<String> json = dataset.toJSON();
        //Save the result to mongoDB
        MongoSpark.save(json);

        sparkSession.close();
    }


}
