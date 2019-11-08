package com.doit;

import com.doit.once.pojo.LogBeansProto;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyProduce {
    public static void main(String[] args) {

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092");
        //key的数据类型, 一般为String
        p.setProperty("key.serializer", StringSerializer.class.getName());
        //value的数据类型, 根据实际情况定
        p.setProperty("value.serializer", ByteArraySerializer.class.getName());
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(p);

        LogBeansProto.LogBeans.Builder list = LogBeansProto.LogBeans.newBuilder();
        LogBeansProto.LogBean.Builder bean = LogBeansProto.LogBean.newBuilder();

        //116.480881,39.989410
        bean.setUid("u001");
        bean.setEventId("1");
        bean.setLongitude(116.480881);
        bean.setLatitude(39.989410);

        list.addLogBeans(bean);

        byte[] bytes = list.build().toByteArray();

        producer.send(new ProducerRecord<String, byte[]>("stream", bytes));
        producer.flush();
        producer.close();


    }
}
