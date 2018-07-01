package com.aura.eight.app;

import com.alibaba.fastjson.JSONObject;
import com.aura.eight.model.TOrder;
import com.aura.eight.utils.JedisUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 实现思路：
 * 1、构建JavaStreamingContext，使用KafkaUtils构建JavaPairInputDStream
 * 2、构建以kafka消息体反序列化为TOrder实体构成的JavaDStream对象
 * 3、遍历此JavaDStream，根据消息中的pid对相应的redis缓存做累加操作
 */

public class TotalPriceGroupByWeekCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("TPGWApplication").setMaster("local[*]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));

        String[] topics = {"t_order"};

        String brokers = com.aura.eight.utils.KafkaUtils.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", "TPGWGroup");
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "totalpriceweek::sum";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<>(Arrays.asList(topics)));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String[] weekDays = {"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};
        JavaDStream<TOrder> originalData = kafkaStream.map(i -> JSONObject.parseObject(i._2(), TOrder.class));
        originalData.foreachRDD(rdd -> {
            rdd.foreachPartition(pdata -> {
                int count = 0;
                while (pdata.hasNext()) {
                    count++;
                    TOrder t = pdata.next();
                    Calendar c = Calendar.getInstance();
                    c.setTime(sdf.parse(t.getBuyTime()));
                    String week = weekDays[c.get(Calendar.DAY_OF_WEEK)-1];
                        JedisUtils.hincrByDouble(clickHashKey, week, new BigDecimal(t.getPrice()).multiply(new BigDecimal(t.getQty())).doubleValue());
                }
                System.out.println("torder分区数据处理成功，条数:" + count);
            });
        });
        jsc.start();
        jsc.awaitTermination();
    }
}
