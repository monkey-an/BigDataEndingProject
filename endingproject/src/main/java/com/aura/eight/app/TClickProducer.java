package com.aura.eight.app;


import com.alibaba.fastjson.JSONObject;
import com.aura.eight.model.TClick;
import com.aura.eight.utils.HiveConnectionManager;
import com.aura.eight.utils.JedisUtils;
import com.aura.eight.utils.KafkaUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 实现思路：
 * 1、以hive方式，逐行读取t_click数据
 * 2、在redis中记录offset
 * 3、读出数据后，构造kafka消息，并发送至kafka
 */

public class TClickProducer {
    private static final String HIVE_TCLICK_OFFSET_KEY = "t_click_offset";
    private static final int pageSize = 1;
    private static final String HQL_TEMPLATE = "select * from t_click limit %1$s,"+pageSize;

    public static void main(String[] args) throws InterruptedException, SQLException {
        while(true)
        {
            //确定从hive读取数据的offset
            int offset = 1;
            if(JedisUtils.exists(HIVE_TCLICK_OFFSET_KEY))
            {
                offset = Integer.valueOf(JedisUtils.getString(HIVE_TCLICK_OFFSET_KEY));
            }
            else
            {
                JedisUtils.setString(HIVE_TCLICK_OFFSET_KEY, offset);
            }

            //从hive读取数据
            String hql = String.format(HQL_TEMPLATE, offset);
            Connection conn = HiveConnectionManager.getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(hql);

            while(rs.next())
            {
                //构造kafka消息并写入队列
                TClick t = new TClick();
                try
                {
                    t.setUid(rs.getString("uid"));
                    t.setClickTime(rs.getString("click_time"));
                    t.setPid(rs.getString("pid"));
                    t.setParam(rs.getString("param"));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                KafkaUtils.sendTClickMsg(JSONObject.toJSONString(t));
                System.out.println("tclick消息写入成功");
            }
            HiveConnectionManager.recoverConn();

            //更新从hive读数据的offset
            JedisUtils.setString(HIVE_TCLICK_OFFSET_KEY, offset+pageSize);

            //延迟10浩渺
            Thread.sleep(500L);
        }
    }
}
