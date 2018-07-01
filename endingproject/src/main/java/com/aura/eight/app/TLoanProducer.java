package com.aura.eight.app;


import com.alibaba.fastjson.JSONObject;
import com.aura.eight.model.TLoan;
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

public class TLoanProducer {
    private static final String HIVE_TLOAN_OFFSET_KEY = "t_loan_offset";
    private static final int pageSize = 100;
    private static final String HQL_TEMPLATE = "select * from t_loan where uid<>'uid' limit %1$s,"+pageSize;

    public static void main(String[] args) throws InterruptedException, SQLException {
        while(true)
        {
            //确定从hive读取数据的offset
            int offset = 1;
            if(JedisUtils.exists(HIVE_TLOAN_OFFSET_KEY))
            {
                offset = Integer.valueOf(JedisUtils.getString(HIVE_TLOAN_OFFSET_KEY));
            }
            else
            {
                JedisUtils.setString(HIVE_TLOAN_OFFSET_KEY, offset);
            }

            //从hive读取数据
            String hql = String.format(HQL_TEMPLATE, offset);
            Connection conn = HiveConnectionManager.getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(hql);

            while(rs.next())
            {
                //构造kafka消息并写入队列
                TLoan t = new TLoan();
                try
                {
                    t.setUid(rs.getString("uid"));
                    t.setLoan_time(rs.getString("loan_time"));
                    t.setLoan_amount(rs.getString("loan_amount"));
                    t.setPlannum(rs.getString("plannum"));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                KafkaUtils.sendTLoanMsg(JSONObject.toJSONString(t));
                System.out.println("tloan消息写入成功");
            }
            HiveConnectionManager.recoverConn();

            //更新从hive读数据的offset
            JedisUtils.setString(HIVE_TLOAN_OFFSET_KEY, offset+pageSize);

            //延迟10浩渺
            Thread.sleep(500L);
        }
    }
}
