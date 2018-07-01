package com.aura.eight.app;

import com.alibaba.fastjson.JSONObject;
import com.aura.eight.model.TLoan;
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
 * 2、构建以kafka消息体反序列化为TLoan实体构成的JavaDStream对象
 * 3、遍历此JavaDStream，根据消息中的pid对相应的redis缓存做累加操作
 */

public class LoanGroupByMonthCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("LGMApplication").setMaster("local[*]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));

        String[] topics = {"t_loan"};

        String brokers = com.aura.eight.utils.KafkaUtils.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", "LGMGroup");
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "loanamounthour::sum";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<>(Arrays.asList(topics)));

        JavaDStream<TLoan> originalData = kafkaStream.map(i -> JSONObject.parseObject(i._2(), TLoan.class));
        originalData.foreachRDD(rdd -> {
            rdd.foreachPartition(pdata -> {
                int count = 0;
                while (pdata.hasNext()) {
                    count++;
                    TLoan t = pdata.next();
                    Calendar c = Calendar.getInstance();
                    c.setTime(sdf.parse(t.getLoan_time()));

                        JedisUtils.hincrByDouble(clickHashKey, c.get(Calendar.HOUR_OF_DAY)+"", new BigDecimal(t.getLoan_amount()).doubleValue());
                }
                System.out.println("tloan分区数据处理成功，条数:" + count);
            });
        });
        jsc.start();
        jsc.awaitTermination();
    }
}
