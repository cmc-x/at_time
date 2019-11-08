package com.doit.pv_uv.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class WriteToRedis extends RichSinkFunction<Tuple3<String, String, String>> {
    private Jedis jedis ;
    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("192.168.152.3", 6379);
        jedis.auth("123456");
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
        if (jedis == null) {
            jedis = new Jedis("192.168.152.3", 6379);
            jedis.auth("123456");
        }
        jedis.hset(value.f0, value.f1, value.f2);
    }
}
