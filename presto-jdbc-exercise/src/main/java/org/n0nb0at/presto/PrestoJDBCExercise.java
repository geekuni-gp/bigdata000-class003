package org.n0nb0at.presto;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class PrestoJDBCExercise {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrestoJDBCExercise.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            LOGGER.error("args length cannot be 0");
            System.exit(-1);
        }
        int connect;

        String queryStr = args[0];

        // 加载JDBC Driver类。
        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Failed to load presto jdbc driver.", e);
            System.exit(-1);
        }
        Connection connection = null;
        Statement statement = null;
        try {
            String url = "jdbc:presto://106.15.194.185:9090/hive/default";
            Properties properties = new Properties();
            properties.setProperty("user", "hadoop");
            // 创建连接对象。
            connection = DriverManager.getConnection(url, properties);
            // 创建Statement对象。
            statement = connection.createStatement();
            // 执行查询。
            ResultSet rs = statement.executeQuery(queryStr);
            // 获取结果。
            int columnNum = rs.getMetaData().getColumnCount();
            int rowIndex = 0;
            while (rs.next()) {
                rowIndex++;
                for (int i = 1; i <= columnNum; i++) {
                    LOGGER.info("Row " + rowIndex + ", Column " + i + ": " + rs.getInt(i));
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Exception thrown.", e);
        } finally {
            // 销毁Statement对象。
            if (statement != null) {
                try {
                    statement.close();
                } catch (Throwable t) {
                    // No-ops
                }
            }
            // 关闭连接。
            if (connection != null) {
                try {
                    connection.close();
                } catch (Throwable t) {
                    // No-ops
                }
            }
        }
    }
}
