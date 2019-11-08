package com.doit.test.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtil {

    /**
     * 创建一个KafkaDataStream
     * @param env Flink对象
     * @param args 传入的程序运行的参数
     * @param dls   序列化和反序列化的方式
     * @param <T>   KafKa消费者读取的类型
     * @return  KafkaDataStream
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> DataStream<T> getKafkaSource(StreamExecutionEnvironment env, String[] args, Class<? extends DeserializationSchema<T>> dls) throws IllegalAccessException, InstantiationException {

        //这个可以解析String数组, 根据String数组中的key得到value
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //可以读取多个topic, 用逗号切分
        List<String> topics = Arrays.asList(parameterTool.get("topics").split(","));

        //取得配置对象, 配置Kafka消费者属性
        Properties pro = parameterTool.getProperties();
        //以前没有记录偏移量，就从头读，如果记录过偏移量，就接着读
        pro.setProperty("auto.offset.reset", "earliest");
        //不自动提交偏移量，让flink提交偏移量
        pro.setProperty("enable.auto.commit", "false");

        //创建一个Kafka消费者
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topics,
                dls.newInstance(),
                pro
        );
        //利用Kafka消费者读取数据
        DataStreamSource<T> source = env.addSource(kafkaConsumer);
        return source ;
    }


    /**
     * 需要对env对象设置的属性有
     * 1.开启checkpoint, 设置多长时间做一次checkpoint, 和模式(必须设置成 EXACTLY_ONCE)
     *      EXACTLY_ONCE模式会保证数据计算并保存完毕后才会将Kafka的offset保存起来
     * 2.设置checkpoint路径
     * 3.设置相邻两次checkpoint的时间间隔
     * 4.设置checkpoint的存储个数
     * 5.任务停掉后设置保存checkpoint的文件
     * 6.设置重启策略
     *
     * @param env
     * @param args
     */
    public static void setEnv(StreamExecutionEnvironment env, String[] args) {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        //1.开启checkPoint
        env.enableCheckpointing(parameter.getLong("ck_time", 5000L), CheckpointingMode.EXACTLY_ONCE);

        //2.设置checkpoint的StateBackend  就是保存在哪里的意思
        env.setStateBackend(new FsStateBackend("file:///data/ck"));

        //3.设置相邻两次checkpoint的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        //4.设置checkpoint的存储个数(其实我认为这是设置同时能进行多少个checkPoint操作)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        //5.任务停掉后设置保存checkpoint的文件
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);;

        //6.设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(parameter.getInt("restart_count"), 5000L));
    }
}
