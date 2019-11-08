package com.doit.pv_uv.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.doit.once.util.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ReadEventDIC extends RichSourceFunction<Tuple2<String, String>> {

    //数据库连接
    private Connection conn = null;
    private DataSource dataSource = null;
    private void getConn() throws Exception {
        Properties props = new Properties();
        props.put("driverClassName", "com.mysql.jdbc.Driver");
        props.put("url", "jdbc:mysql://localhost:3306/demo");
        props.put("username", "root");
        props.put("password", "123456");
        dataSource = DruidDataSourceFactory.createDataSource(props);
    }

    private boolean flag = true;

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        getConn();
        String lastUpdate = null;
        String sql = "select * from tb_event_name where last_update > ? order by last_update DESC ";
        while (flag) {
            if (conn == null) {
                conn = dataSource.getConnection();
            }

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, lastUpdate == null ? "2000-09-02 17:09:59" : lastUpdate);
            ResultSet resultSet = ps.executeQuery();
            int index = 0;
            while(resultSet.next()){
                String event_id = resultSet.getString("event_id");
                String event_name = resultSet.getString("event_name");
                if (index == 0) {
                    lastUpdate = resultSet.getString("last_update");
                    index++;
                }
                ctx.collect(Tuple2.of(event_id, event_name));
            }
            Thread.sleep(5000L);
        }
    }

    @Override
    public void cancel() {
        try {
            flag = false;
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
