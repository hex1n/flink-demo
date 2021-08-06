package com.dlwlrma.flink.api.demo;

import com.mysql.jdbc.Driver;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author hex1n
 * @Date 2021/8/3 21:27
 * @Description
 */
public class JdbcReader1 extends RichSourceFunction<List<TenantDB>> {

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
    public void run(SourceContext<List<TenantDB>> sourceContext) throws Exception {
        try {
            List<TenantDB> tenantDbList = Lists.newArrayList();
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String s = resultSet.getString("");
                    String s1 = resultSet.getString("");
                    System.out.println("============");
                    sourceContext.collect(tenantDbList);
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
}
