package com.aura.eight.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtils {
    private static int MAX_IDLE = 200;
    private static int TIMEOUT = 10000;
    private static boolean TEST_ON_BORROW = true;
    private static final String REDIS_SERVER = "anlu.local";
    private static final int REDIS_PORT = 6379;

    private static JedisPool pool = null;

    private static JedisPoolConfig config() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(MAX_IDLE);
        config.setTestOnBorrow(TEST_ON_BORROW);
        return config;
    }

    private static JedisPool get() {
        if(pool == null) {
            pool = new JedisPool(config(),REDIS_SERVER,REDIS_PORT,TIMEOUT);
        }
        return pool;
    }

    public static void hincrBy(String key,String field,int count){
        Jedis jedis = get().getResource();
        jedis.hincrBy(key, field, count);
        jedis.close();
    }

    public static void hincrByDouble(String key,String field,double count){
        Jedis jedis = get().getResource();
        jedis.hincrByFloat(key, field, count);
        jedis.close();
    }

    public static String getString(String key)
    {
        Jedis jedis = get().getResource();
        String result = jedis.get(key);
        jedis.close();
        return result;
    }

    public static void setString(String key,Object value)
    {
        Jedis jedis = get().getResource();
        jedis.set(key, value.toString());
        jedis.close();
    }

    public static boolean exists(String key)
    {
        Jedis jedis = get().getResource();
        boolean result = jedis.exists(key);
        jedis.close();
        return result;
    }
}
