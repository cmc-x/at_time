package com.doit.once.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
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
    private  Connection conn = null;

    private void getConn() throws Exception {
        Properties props = new Properties();
        props.put("driverClassName", "com.mysql.jdbc.Driver");
        props.put("url", "jdbc:mysql://localhost:3306/demo");
        props.put("username", "root");
        props.put("password", "123456");
        DataSource dataSource = DruidDataSourceFactory.createDataSource(props);
        conn = dataSource.getConnection();
    }

    boolean flag = true ;
    /**
     * 只会调用一次, 一直输出元素
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        String sql = "select event_id, event_name, last_update from event_table where last_update > ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        String lastUpdate = null;
        while(flag) {
            ps.setString(1, lastUpdate == null ? "1999-09-01 22:36:30" : lastUpdate);
            ResultSet resultSet = ps.executeQuery();
            String eventId = resultSet.getString(1);
            String eventName = resultSet.getString(2);
            lastUpdate = resultSet.getString(3);
            ctx.collect(Tuple2.of(eventId, eventName));
        }
    }

    @Override
    public void cancel() {
        flag = false;
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
