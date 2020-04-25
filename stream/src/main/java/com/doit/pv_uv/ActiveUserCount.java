package com.doit.pv_uv;

import com.doit.once.util.FlinkUtils;
import com.doit.pv_uv.pojo.EventUserCountBean;
import com.doit.pv_uv.util.ReadEventDIC;
import com.doit.pv_uv.util.WriteToRedis;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class ActiveUserCount {
    public static void main(String[] args) throws Exception {
        
        //读取Kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkUtils.setEnvProperties(env, args);

        DataStreamSource<String> kafkaSource = FlinkUtils.getKafkaSource(env, args, SimpleStringSchema.class);

        //封装成EventUserCountBean对象
        SingleOutputStreamOperator<EventUserCountBean> beanDS = kafkaSource.process(new ProcessFunction<String, EventUserCountBean>() {
            @Override
            public void processElement(String line, Context ctx, Collector<EventUserCountBean> out) throws Exception {
                try {
                    String[] strs = line.split(",");
                    EventUserCountBean bean = new EventUserCountBean(strs[0], strs[1], strs[2], strs[3], strs[4]);
                    bean.setCount(1);
                    out.collect(bean);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("te2");
                    System.out.println("te1");
                    System.out.println("te2");
                    System.out.println("a2");
                    System.out.println("a1");
                    System.out.println("a2");
                }
            }
        });

        //todo:集成事件名称
        //准备广播变量描述器
        MapStateDescriptor<String, Tuple2<String, String>> event_name_dic = new MapStateDescriptor<>(
                "event_name_dic",
                Types.STRING,
                Types.TUPLE(Types.STRING, Types.STRING)
        );
        //1.获取mysql中的字典库, 并广播出去
        BroadcastStream<Tuple2<String, String>> event_broadcast = env.addSource(new ReadEventDIC()).broadcast(event_name_dic);

        //2.关联字典库
        SingleOutputStreamOperator<EventUserCountBean> collEventName = beanDS.connect(event_broadcast).process(new BroadcastProcessFunction<EventUserCountBean, Tuple2<String, String>, EventUserCountBean>() {
            @Override
            public void processElement(EventUserCountBean bean, ReadOnlyContext ctx, Collector<EventUserCountBean> out) throws Exception {
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(event_name_dic);
                String eventTypeId = bean.getEventTypeId();
                String f1 = broadcastState.get(eventTypeId).f1;
                System.out.println(eventTypeId + "-> " +f1);
                bean.setEventTypeName(f1);
                out.collect(bean);
            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<EventUserCountBean> out) throws Exception {
                BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(event_name_dic);
                broadcastState.put(value.f0, value);
                System.out.println(value);
            }
        });

        //按活动名称统计事件人数
        KeyedStream<EventUserCountBean, Tuple> keyByEventName = collEventName.keyBy("eventName", "eventTypeName", "uid");
        //保存到redis
        keyByEventName.sum("count").map(new MapFunction<EventUserCountBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(EventUserCountBean bean) throws Exception {
                //外部key:eventName|time|
                System.out.println(bean);
                return Tuple3.of(bean.getEventName()+"|"+bean.getTime().split(" ")[0],
                        bean.getEventTypeName(),
                        bean.getCount()+"");
            }
        }).addSink(new WriteToRedis());




        env.execute();
    }

}
