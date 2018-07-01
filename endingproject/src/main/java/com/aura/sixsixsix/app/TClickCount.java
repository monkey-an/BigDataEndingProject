package com.aura.sixsixsix.app;

import com.alibaba.fastjson.JSONObject;
import com.aura.sixsixsix.model.TClick;
import com.aura.sixsixsix.utils.JedisUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * 实现思路：
 * 1、构建JavaStreamingContext，使用KafkaUtils构建JavaPairInputDStream
 * 2、构建以kafka消息体反序列化为TClick实体构成的JavaDStream对象
 * 3、遍历此JavaDStream，根据消息中的pid对相应的redis缓存做累加操作
 */

public class TClickCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("TClickCount").setMaster("local[*]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));

        // Kafka configurations
        String[] topics = {"t_click"};

        String brokers = com.aura.sixsixsix.utils.KafkaUtils.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "clickpid::count";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<>(Arrays.asList(topics)));

        JavaDStream<TClick> originalData = kafkaStream.map(i -> JSONObject.parseObject(i._2(), TClick.class));
        originalData.foreachRDD(rdd ->
                rdd.foreachPartition(pdata -> {
                    int count = 0;
                    while(pdata.hasNext()) {
                        count++;
                        TClick t = pdata.next();
                        JedisUtils.hincrBy(clickHashKey, "click"+t.getPid(), 1);
                    }
                    System.out.println("tclick分区数据处理成功，条数:"+count);
                }));
        jsc.start();
        jsc.awaitTermination();
    }
}
