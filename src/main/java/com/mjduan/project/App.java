package com.mjduan.project;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Hans on 2017-06-01 07:12
 */
public class App {
    private static final String URL = "jdbc:mysql://localhost:3306/mybank2?useUnicode=true&characterEncoding=UTF-8";

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark-mysql");
        sparkConf.setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", "root");//username
        connectionProperties.setProperty("password", "123");// password

        Dataset<Row> mypay = sqlContext.read().jdbc(URL, "t_mypay", connectionProperties);
        mypay.show(3);

        sparkContext.close();
    }

}
