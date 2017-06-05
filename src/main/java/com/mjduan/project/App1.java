package com.mjduan.project;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import static org.apache.spark.sql.functions.col;

/**
 * Hans on 2017-06-01 07:12
 */
public class App1 {
    private static final String URL = "jdbc:mysql://localhost:3306/mybank2?useUnicode=true&characterEncoding=UTF-8";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark-mysql");
        sparkConf.setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", "root");//username
        connectionProperties.setProperty("password", "123");// password

        Dataset<Row> mypay = sqlContext.read().jdbc(URL, "t_mypay", connectionProperties);
        //Just print three records
        mypay.show(3);
        System.out.println("\nThe schema:");
        //Print the structure of table 't_mypay'
        mypay.printSchema();

        /** Select only the 'remark' column **/
        System.out.println("\nThe remark col:");
        mypay.select("remark").limit(10).show();

        System.out.println("\n ");
        mypay.select(col("remark"), col("id").plus(1)).limit(5).show();

        //Select record that money more than 40, then print them order by 'time' column
        mypay.filter(col("money").gt(40)).orderBy(col("time")).show();

        //Print the count
        long count = mypay.filter(col("money").gt(40)).count();
        System.out.println("count:" + count);

        sparkContext.close();
    }

}
