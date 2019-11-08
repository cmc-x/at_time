package com.doit.once.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class MyByteArraySerialize implements DeserializationSchema<byte[]> {

    /**
     *  方序列化
     * @param message 从Kafka读取出来的数据
     * @return 序列化之后的数据
     * @throws IOException ...
     */
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        return message;
    }

    /**
     * 未知功能
     * @param nextElement 读取的下一条数据
     * @return 是否为最后一条
     */
    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    /**
     * 描述返回值类型
     * @return 序列化之后的类型
     */
    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }

}
