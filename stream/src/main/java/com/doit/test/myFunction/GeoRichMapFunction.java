package com.doit.test.myFunction;

import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSONObject;
import com.doit.test.pojo.EventLogBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class GeoRichMapFunction extends RichMapFunction<EventLogBean, EventLogBean> {
    private Jedis jedis = null;

    /**
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //打开jedis连接
        jedis = new Jedis("192.168.152.3", 6379);
        jedis.auth("123456");
    }



    /**
     * 集成省市区信息
     * @param bean 日志对象
     * @return  集成省市区信息后的日志对象
     * @throws Exception ...
     */
    @Override
    public EventLogBean map(EventLogBean bean) throws Exception {
        //判断redis连接是否存在
        if (jedis == null) {
            jedis = new Jedis("192.168.152.3", 6379);
            jedis.auth("123456");
        }

        //1.生成geoHash, 查询redis数据库中的省市区字典
        double jd = bean.getJd();
        double wd = bean.getWd();
        String geoHash = GeoHash.withCharacterPrecision(wd, jd, 5).toBase32();
        String message = jedis.hget("geoHashDIC", geoHash);
        //如果字典库没有, 请求高德
        if (message == null || "".equals(message)) {
            //获取Http连接
            URL url = new URL("https://restapi.amap.com/v3/geocode/regeo?output=JSON&location=" + jd + "," + wd + "&key=0e483bcd380d13ec3fadf56df7aa8dc5&radius=1000&extensions=all");
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            InputStream in = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String jsonStr = br.readLine();
            br.close();
            in.close();

            JSONObject jsonobj = JSONObject.parseObject(jsonStr);
            JSONObject regeocode = jsonobj.getJSONObject("regeocode");
            JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
            String province = addressComponent.getString("province");
            String city = "";
            try {
                city = addressComponent.getString("city");
            } catch (Exception e) {
                e.printStackTrace();
            }
            String district = addressComponent.getString("district");
            bean.setProvince(province);
            bean.setCity(city);
            bean.setArea(district);
            //更新字典库
            jedis.hset("geoHashDIC", geoHash, province+","+city+","+district);

        }else {
            String[] split = message.split(",");
            bean.setProvince(split[0]);
            bean.setCity(split[1]);
            bean.setArea(split[2]);
        }

        return bean;
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }

}
