package org.n0nb0at.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOperation {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) {
        try {
            // 初始化连接
            init();

            // 创建表
            createTable("guopeng:student", "info", "score");

            // 更新字段信息
            /*insertData("guopeng:student", "guopeng", "info", "student_id", "20190343020028");
            insertData("guopeng:student", "guopeng", "info", "class", "3");
            insertData("guopeng:student", "guopeng", "score", "underStanding", "60");
            insertData("guopeng:student", "guopeng", "score", "programing", "80");*/

            // 查询行信息：可指定列族、列名
            getData("guopeng:student", "guopeng", StringUtils.EMPTY, StringUtils.EMPTY);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭连接
            close();
        }
    }

    public static void init() {
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.rootdir", "hdfs://master01:9000/hbase");
//        configuration.set("hbase.zookeeper.quorum", "master01,slave01,slave02");
        configuration.set("hbase.rootdir", "hdfs://jikehadoop01:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "47.101.216.12");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String myTableName, String... colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
        } else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for (String str : colFamily) {
                ColumnFamilyDescriptor family =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
        }
    }

    public static void insertData(String tableName, String rowKey, String colFamily,
                                  String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
        System.out.printf("insert success! rowKey:%s, colFamily:%s, col:%s val:%s %n",
                rowKey, colFamily, col, val);
    }

    public static void getData(String tableName, String rowKey, String colFamily,
                               String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        if (!StringUtils.isEmpty(colFamily)) {
            if (!StringUtils.isEmpty(col)) {
                get.addColumn(colFamily.getBytes(), col.getBytes());
            } else {
                get.addFamily(colFamily.getBytes());
            }
        }
        Result result = table.get(get);
        System.out.printf("getData: %s%n", result.toString());
        table.close();
    }
}
