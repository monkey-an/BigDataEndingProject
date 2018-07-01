package com.aura.eight.app;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * 实现思路：
 * 1、以csv格式，从hdfs读入t_loan_sum数据
 * 2、以csv格式，从hdfs读入t_order数据
 * 3、以tLoanSum为基础数据，创建视图tableLoan
 * 4、以tOrder为基础数据，创建视图tableOrder
 * 5、对tableLoan和tableOrder进行查询，使用SparkSQL方式查询出符合条件的结果数据并逐一输出
 * 6、查询语句：select * from tableOrder
 *              where not exists (select tableLoan.uid from tableLoan where tableLoan.uid=tableOrder.uid)
 *              and not exists (select t.uid from tableOrder t where t.uid=tableOrder.uid and  discount<>'0')
 */

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
