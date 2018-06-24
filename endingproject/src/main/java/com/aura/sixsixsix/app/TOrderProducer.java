package com.aura.sixsixsix.app;


import com.alibaba.fastjson.JSONObject;
import com.aura.sixsixsix.model.TOrder;
import com.aura.sixsixsix.utils.HiveConnectionManager;
import com.aura.sixsixsix.utils.JedisUtils;
import com.aura.sixsixsix.utils.KafkaUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TOrderProducer {
    private static final String HIVE_TORDER_OFFSET_KEY = "t_order_offset";
    private static final int pageSize = 1;
    private static final String HQL_TEMPLATE = "select * from t_order limit %1$s,"+pageSize;

    public static void main(String[] args) throws InterruptedException, SQLException {
        while(true)
        {
            //确定从hive读取数据的offset
            int offset = 1;
            if(JedisUtils.exists(HIVE_TORDER_OFFSET_KEY))
            {
                offset = Integer.valueOf(JedisUtils.getString(HIVE_TORDER_OFFSET_KEY));
            }
            else
            {
                JedisUtils.setString(HIVE_TORDER_OFFSET_KEY, offset);
            }

            //从hive读取数据
            String hql = String.format(HQL_TEMPLATE, offset);
            Connection conn = HiveConnectionManager.getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(hql);

            while(rs.next())
            {
                //构造kafka消息并写入队列
                TOrder t = new TOrder();
                try
                {
                    t.setUid(rs.getString("uid"));
                    t.setBuyTime(rs.getString("buy_time"));
                    t.setPrice(rs.getString("price"));
                    t.setQty(rs.getString("qty"));
                    t.setCateId(rs.getString("cate_id"));
                    t.setDiscount(rs.getString("discount"));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                KafkaUtils.sendTOrderMsg(JSONObject.toJSONString(t));
                System.out.println("torder消息写入成功");
            }
            HiveConnectionManager.recoverConn();

            //更新从hive读数据的offset
            JedisUtils.setString(HIVE_TORDER_OFFSET_KEY, offset+pageSize);

            //延迟10浩渺
            Thread.sleep(500L);
        }
    }
}
