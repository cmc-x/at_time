package com.doit.once.util;

import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSONObject;
import com.doit.once.pojo.LogBeansProto;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class ProvinceMapFunction extends RichMapFunction<LogBeansProto.LogBean, LogBeansProto.LogBean> {

    private Jedis jedis = null;

    /**
     * 只调用一次, 在Task开始时调用
     * @param parameters 配置参数
     * @throws Exception ..
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //连接redis
        jedis = new Jedis("192.168.152.3", 6379);
        jedis.auth("123456");

    }

    @Override
    public void close() throws Exception {
    }

    /**
     * 关联省市区维度信息
     * @param bean 传入的bean
     * @return 返回关联省市区之后的对象
     * @throws Exception ..
     */
    @Override
    public LogBeansProto.LogBean map(LogBeansProto.LogBean bean) throws Exception {

        if (jedis == null) {
            jedis = new Jedis("192.168.152.3", 6379);
            jedis.auth("123456");
        }

        LogBeansProto.LogBean.Builder builder = bean.toBuilder();
        //取出经纬度
        double longitude = bean.getLongitude();
        double latitude = bean.getLatitude();

        //根据经纬度计算geoHash值
        String geoHash = GeoHash.withCharacterPrecision(latitude, longitude, 5).toBase32();
        String message = jedis.hget("geoHash", geoHash);

        //判断字典库中是否存在
        if(message == null || "".equals(message)) {
            //请求高德
            URL url = new URL("https://restapi.amap.com/v3/geocode/regeo?output=JSON&location=" + longitude + "," + latitude + "&key=0e483bcd380d13ec3fadf56df7aa8dc5&radius=1000&extensions=all");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            InputStream in = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String resJson = br.readLine();

            JSONObject jsonObj = JSONObject.parseObject(resJson);
            JSONObject addressComponent = jsonObj.getJSONObject("regeocode").getJSONObject("addressComponent");

            String province = addressComponent.getString("province");
            String city = "";
            try {
                city = addressComponent.getString("city");
            } catch (Exception e) {
                e.printStackTrace();
            }
            String district = addressComponent.getString("district");

            builder.setProvince(province);
            builder.setCity(city);
            builder.setDistrict(district);

            //存入redis
            jedis.hset("geoHash", geoHash, province+","+city+","+district);
        }else {
            String[] split = message.split(",");
            builder.setProvince(split[0]);
            builder.setCity(split[1]);
            builder.setDistrict(split[2]);
        }

        LogBeansProto.LogBean newBean = builder.build();
        return newBean;
    }
}
