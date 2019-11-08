package com.doit.test.data_predeal;

import com.alibaba.fastjson.JSON;
import com.doit.test.myFunction.GeoRichMapFunction;
import com.doit.test.pojo.EventLogBean;
import com.doit.test.util.FlinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.ZoneId;

public class DataSort {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        //创建一个Flink执行对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置evn的各种属性: checkpoint的属性等
        FlinkUtil.setEnv(env, args);

        //根据工具类得到一个KafkaSource对象, 传入env, args, 序列化类的Class对象
        DataStream<String> kafkaSource = FlinkUtil.getKafkaSource(env, args, SimpleStringSchema.class);

        //将数据封装到bean对象中
        SingleOutputStreamOperator<EventLogBean> eventBeanDS = kafkaSource.map(new MapFunction<String, EventLogBean>() {
            @Override
            public EventLogBean map(String line) throws Exception {
                EventLogBean eventLogBean = null;
                try {
                    eventLogBean = JSON.parseObject(line, EventLogBean.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return eventLogBean;
            }
        })
                //过滤掉null
                .filter(new FilterFunction<EventLogBean>() {
            @Override
            public boolean filter(EventLogBean bean) throws Exception {
                return bean != null;
            }
        });

        SingleOutputStreamOperator<EventLogBean> collDS = eventBeanDS.map(new GeoRichMapFunction());

        //集成省市区信息
       /* SingleOutputStreamOperator<EventLogBean> collDS = eventBeanDS.map(new MapFunction<EventLogBean, EventLogBean>() {
            @Override
            public EventLogBean map(EventLogBean bean) throws Exception {
                double jd = bean.getJd();
                double wd = bean.getWd();
                String geoHash = GeoHash.withCharacterPrecision(wd, jd, 5).toBase32();
                Jedis redis = RedisUtil.getRedis();

                String message = redis.hget("geoHash", geoHash);

                //如果字典库中没有, 则使用高德请求
                if (message == null || "".equals(message)) {
                    URL url = new URL("https://restapi.amap.com/v3/geocode/regeo?output=JSON&location=" + jd + "," + wd + "&key=0e483bcd380d13ec3fadf56df7aa8dc5&radius=1000&extensions=all");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    InputStream in = conn.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String json = br.readLine();

//                    System.out.println("json" + json);
                    JSONObject jsonobj = JSONObject.parseObject(json);
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
                    //存入redis
                    redis.hset("geoHash", geoHash, province + "," + city + "," + district);
                } else {
                    String[] split = message.split(",");
                    bean.setProvince(split[0]);
                    bean.setCity(split[1]);
                    bean.setArea(split[2]);
                }
                return bean;
            }
        });*/

        //创建两个流
         OutputTag<String> flow = new OutputTag<String>("flow"){};
         OutputTag<String> acitiv = new OutputTag<String>("acitiv"){};

        //分流 - acitiv  flow
        SingleOutputStreamOperator<String> allDS = collDS.process(new ProcessFunction<EventLogBean, String>() {
            @Override
            public void processElement(EventLogBean bean, Context ctx, Collector<String> out) throws Exception {
                String s = JSON.toJSONString(bean);
                if (bean.getEventType().startsWith("flow")) {
                    ctx.output(flow, s); //侧流输出 - flow
                } else if (bean.getEventType().startsWith("acitiv")) {
                    ctx.output(acitiv, s); //侧流输出 - acitiv
                }
                //主流输出
                out.collect(s);
            }
        });

        //flow流输出到Kafka的flow topic
        allDS.getSideOutput(flow).addSink(new FlinkKafkaProducer<String>(
                "flow",
                new SimpleStringSchema(),
                ParameterTool.fromArgs(args).getProperties()
        ));

        //acitiv流输出到Kafka的acitiv topic
        allDS.getSideOutput(acitiv).addSink(new FlinkKafkaProducer<String>(
                "acitiv",
                new SimpleStringSchema(),
                ParameterTool.fromArgs(args).getProperties()
        ));

        //todo:将主流存放到HDFS
        //创建一个HDFS Sink
        BucketingSink<String> hdfsSink = new BucketingSink<>("hdfs://linux01:9000/stream/");
        //设置每隔一小时创建一个新的文件夹
        hdfsSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai")));
        //设置写的格式
        hdfsSink.setWriter(new StringWriter<String>());
        //每128M写一个文件
        hdfsSink.setBatchSize(1024 * 1024 * 128); // this is 200 MB,
        //每隔30秒写一个文件
        hdfsSink.setBatchRolloverInterval(3000 * 1000); // this is 30 sec
        //写
        allDS.addSink(hdfsSink);

        //执行flink程序
        env.execute();


    }
}
