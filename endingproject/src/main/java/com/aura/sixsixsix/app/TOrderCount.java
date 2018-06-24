package com.aura.sixsixsix.app;

import com.alibaba.fastjson.JSONObject;
import com.aura.sixsixsix.model.TClick;
import com.aura.sixsixsix.model.TOrder;
import com.aura.sixsixsix.utils.HiveConnectionManager;
import com.aura.sixsixsix.utils.JedisUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TOrderCount {
    private static final String HQL_TEMPLATE = "select age from t_user where uid=%1$S";


    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("TOrderSum").setMaster("local[*]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));

        String[] topics = {"t_order"};

        String brokers = com.aura.sixsixsix.utils.KafkaUtils.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "buyage::sum";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<>(Arrays.asList(topics)));

        JavaDStream<TOrder> originalData = kafkaStream.map(i -> JSONObject.parseObject(i._2(), TOrder.class));
        originalData.foreachRDD(rdd -> {
            rdd.foreachPartition(pdata -> {
                Connection conn = HiveConnectionManager.getNewConn();
                int count = 0;
                while (pdata.hasNext()) {
                    count++;
                    TOrder t = pdata.next();
                    ResultSet rs = conn.createStatement().executeQuery(String.format(HQL_TEMPLATE, t.getUid()));
                    String ageStr = "";
                    if (rs.next()) {
                        ageStr = rs.getString("age");
                    }
                    if (!"".equals(ageStr))
                        JedisUtils.hincrByDouble(clickHashKey, "buy" + ageStr, new BigDecimal(t.getPrice()).multiply(new BigDecimal(t.getQty())).doubleValue());
                }
                System.out.println("torder分区数据处理成功，条数:" + count);
                conn.close();
            });
        });
        jsc.start();
        jsc.awaitTermination();
    }
}
