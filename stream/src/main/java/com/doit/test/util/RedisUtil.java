package com.doit.test.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        jedisPool = new JedisPool("192.168.152.3", 6379);
    }

    /**
     * 取得redis连接
     * @return redis连接对象
     */
    public static Jedis getRedis() {
        Jedis jedis = jedisPool.getResource();
        jedis.auth("123456");
        return jedis;
    }
}
