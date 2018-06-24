package com.aura.sixsixsix.app;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Case42 {
    public static void main(String[] args)
    {
        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]");
        sc.setAppName("exam-case4-2");
        SparkSession ss = SparkSession.builder().config(sc).getOrCreate();

        Dataset<Row> tLoanSumDS = ss.read().format("csv").option("header", "true").csv("hdfs://anlu.local:9000/exam/t_loan_sum.csv");
        Dataset<Row> tOrderDS = ss.read().option("header", "true").csv("hdfs://anlu.local:9000/exam/t_order.csv");

        tLoanSumDS.createOrReplaceTempView("tableLoan");
        tOrderDS.createOrReplaceTempView("tableOrder");

        ss.sql("select * " +
                "from tableOrder " +
                "where not exists (select tableLoan.uid from tableLoan where tableLoan.uid=tableOrder.uid) " +
                "and not exists (select t.uid from tableOrder t where t.uid=tableOrder.uid and  discount<>'0')")
                .show();

        ss.stop();
    }
}
