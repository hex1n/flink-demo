package com.dlwlrma.flink.api.demo;

import com.mysql.jdbc.Driver;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.scala.map;
import org.apache.flink.table.expressions.E;
import org.joda.time.LocalDateTime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author hex1n
 * @Date 2021/8/3 21:27
 * @Description
 */
public class JdbcReader extends RichSourceFunction<List<List<String>>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private List<TenantDB> tenantDBS;

    // 该方法主要用于打开数据库连接,下面的ConfigKeys类是获取配置的类


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DriverManager.registerDriver(new Driver());
        String db_url = "jdbc:mysql://10.100.0.210:3306/dat_tenant";
        String userName = "yzl";
        String password = "yzl@all20201125";
        connection = DriverManager.getConnection(db_url, userName, password);
        ps = this.connection.prepareStatement(" select tenant_id,db_url,user_name,password from t_tenant_db where service_name='bi-service'");

    }

    @Override
    public void run(SourceContext<List<List<String>>> sourceContext) throws Exception {
        try {
            List<List<String>> list = Lists.newArrayList();
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String s = resultSet.getString("");
                    String s1 = resultSet.getString("");
                    System.out.println("============");
                    sourceContext.collect(list);
                    list.clear();
                    // 五分钟清空一下数据
                    Thread.sleep(5000 * 60);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning = false;
    }

    public static void main(String[] args) {
        int i = ChinaTimeUtils.dayDiff(System.currentTimeMillis(), new Date("2021-08-04").getTime());
        System.out.println(i);
    }
}
