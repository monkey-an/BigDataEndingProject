package com.aura.sixsixsix.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class Case41 {
    public static void main(String[] args)
    {
        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]");
        sc.setAppName("exam-case4-1");
        SparkSession ss = SparkSession.builder().config(sc).getOrCreate();

        Dataset<Row> tLoanSumDS = ss.read().format("csv").option("header", "true").csv("hdfs://anlu.local:9000/exam/t_loan_sum.csv");
        Dataset<Row> tOrderDS = ss.read().option("header", "true").csv("hdfs://anlu.local:9000/exam/t_order.csv");

        Dataset<Row> groupByEdOrderDs = tOrderDS.groupBy("uid").agg(sum(col("price").multiply(col("qty"))).alias("totalPrice"));

        tLoanSumDS
                .join(groupByEdOrderDs,"uid")
                .filter(col("loan_sum").gt(5L))
                .filter(col("totalPrice").gt(col("loan_sum")))
                .select(col("uid"))
                .foreach((ForeachFunction<Row>) row -> System.out.println(row.getString(0)));
        ss.stop();
    }
}
