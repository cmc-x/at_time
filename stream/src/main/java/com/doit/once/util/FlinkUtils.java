package com.doit.once.util;

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
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {


    /**
     *  取得KafkaSource
     * @param env Flink对象
     * @param args 传入的配置参数
     * @param cls 序列化和方序列化类的Class对象
     * @param <T> 从Kafka中读取的数据类型
     * @return KafkaSource
     * @throws IllegalAccessException ...
     * @throws InstantiationException ...
     */
    public static <T> DataStreamSource<T> getKafkaSource(StreamExecutionEnvironment env, String[] args, Class<? extends DeserializationSchema<T>> cls) throws IllegalAccessException, InstantiationException {
        ParameterTool para = ParameterTool.fromArgs(args);

        String topics = para.get("topics");
        String[] split = topics.split(",");
        List<String> topicList = Arrays.asList(split);

        Properties properties = para.getProperties();
        //以前没有记录偏移量，就从头读，如果记录过偏移量，就接着读
        properties.setProperty("auto.offset.reset", "earliest");
        //不自动提交偏移量，让flink提交偏移量
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<T>  kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                cls.newInstance(),
                properties
        );

        return env.addSource(kafkaConsumer);
    }


    /**
     * 设置env的运行参数
     * @param env flink对象
     */
    public static void setEnvProperties(StreamExecutionEnvironment env, String[] args) {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        //开启checkpoint
        env.enableCheckpointing(parameter.getLong("ck_time", 5000L), CheckpointingMode.EXACTLY_ONCE);

        //设置checkpoint保存路径
        env.setStateBackend(new FsStateBackend("file:///stream/data/ck"));

        //设置前一次checkpoint完成后开启下一次checkpoint的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        //设置checkpoint的存储个数(其实我认为这是设置同时能进行多少个checkPoint操作)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        //任务停掉后设置保存checkpoint的文件
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置重启策略
        System.out.println(parameter.getInt("restart_count"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(parameter.getInt("restart_count"), 5000L));

    }

}
