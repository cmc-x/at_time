package com.doit.once.data_process;

import com.doit.once.pojo.LogBeansProto;
import com.doit.once.util.FlinkUtils;
import com.doit.once.util.MyByteArraySerialize;
import com.doit.once.util.ProvinceMapFunction;
import com.doit.once.util.ReadEventDIC;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class DataProcess {
    public static void main(String[] args) throws Exception {
        //创建一个Flink对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置env运行参数
        FlinkUtils.setEnvProperties(env, args);

        //获取从Kafka中读取数据的Stream
        DataStreamSource<byte[]> kafkaSource = FlinkUtils.getKafkaSource(env, args, MyByteArraySerialize.class);

        //数据处理 - 将byte[] 封装进bean
        SingleOutputStreamOperator<LogBeansProto.LogBean> logBeanDS = kafkaSource.flatMap(new FlatMapFunction<byte[], LogBeansProto.LogBean>() {
            @Override
            public void flatMap(byte[] line, Collector<LogBeansProto.LogBean> out) throws Exception {
                LogBeansProto.LogBeans logBeans = LogBeansProto.LogBeans.parseFrom(line);
                List<LogBeansProto.LogBean> list = logBeans.getLogBeansList();
                for (LogBeansProto.LogBean logBean : list) {
                    out.collect(logBean);
                }
            }
        });

        //集成省市区
        SingleOutputStreamOperator<LogBeansProto.LogBean> provinceDS = logBeanDS.map(new ProvinceMapFunction());

        //根据事件ID集成事件名称
        //读取mysql中的数据
        DataStreamSource<Tuple2<String, String>> eventDIC = env.addSource(new ReadEventDIC());

        //创建一个广播变量描述器
        MapStateDescriptor<String, Tuple2<String, String>> descriptor = new MapStateDescriptor<>(
                "eventDIC",
                Types.STRING,
                Types.TUPLE(Types.STRING, Types.STRING)
        );

        //广播变量
        BroadcastStream<Tuple2<String, String>> eventDICBro = eventDIC.broadcast(descriptor);

        //集成广播变量中的事件名称
        SingleOutputStreamOperator<LogBeansProto.LogBean> res = provinceDS.connect(eventDICBro).process(new BroadcastProcessFunction<LogBeansProto.LogBean, Tuple2<String, String>, LogBeansProto.LogBean>() {
            /**
             *  根据bean里面的ID取得广播变量对应的值, 并封装进bean里面
             * @param bean  主流当中的bean元素
             * @param ctx   Flink的上下文, 里面可以取得Flink程序中所有的广播变量, 只是需要传进一个广播变量描述器
             * @param out   输出流收集器, 调用collect()方法将需要输出的元素添加进去
             * @throws Exception ..
             */
            @Override
            public void processElement(LogBeansProto.LogBean bean, ReadOnlyContext ctx, Collector<LogBeansProto.LogBean> out) throws Exception {
                ReadOnlyBroadcastState<String, Tuple2<String, String>> eventDICMap = ctx.getBroadcastState(descriptor);
                String eventId = bean.getEventId();
                Tuple2<String, String> tp = eventDICMap.get(eventId);
                LogBeansProto.LogBean newBean = bean.toBuilder().setEventName(tp.f1).build();
                out.collect(newBean);
            }

            /**
             *  更新广播变量
             * @param value 广播变量的流的元素会进入这里
             * @param ctx   Flink的上下文, 里面可以取得Flink程序中所有的广播变量, 只是需要传进一个广播变量描述器
             * @param out   输出流收集器, 调用collect()方法将需要输出的元素添加进去
             * @throws Exception ..
             */
            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<LogBeansProto.LogBean> out) throws Exception {
                BroadcastState<String, Tuple2<String, String>> state = ctx.getBroadcastState(descriptor);
                state.put(value.f0, value);
            }
        });

        res.print();

        env.execute();


    }
}
