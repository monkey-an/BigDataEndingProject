package com.aura.eight.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
/**
 * 实现思路：
 * 1、以csv格式，从hdfs读入t_loan_sum文件中的数据
 * 2、以csv格式，从hdfs读入t_order文件中的数据
 * 3、将tOrder数据按照uid分组，以price和qty的乘积聚合并命名别名为totalPrice
 * 4、将分组得到的uid、totalPrice与tLoanSum按照uid进行join操作，并过滤出借款总和loan_sum符合条件、totalPrice符合条件的uid，并逐行输出
 */

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
