package com.hlbigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * Created by sponge on 15-10-9.
 */
public class PhoneAddress {
    private static final String TABLE_NAME = "phone";
    private static final String CF_NAME = "cf";
    private static final byte[] COL_PROVINCE = "province".getBytes();
    private static final byte[] COL_CITY = "city".getBytes();
    private static final byte[] COL_OPERATOR = "operator".getBytes();

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_NAME).setCompressionType(Compression.Algorithm.SNAPPY));

            System.out.print("Creating table. ");

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
            System.out.println(" Done.");
        }
    }

    private static void insertPhone(Configuration config,
                                    String province, String city,
                                    String operator, String phone) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            Table table = connection.getTable(tableName);
            Put put = new Put(phone.getBytes());
            put.addColumn(CF_NAME.getBytes(), COL_PROVINCE, province.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_CITY, city.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_OPERATOR, operator.getBytes("UTF-8"));

            System.out.println("Insert Data .");
            table.put(put);

            table.close();
        }
    }

    public static void DemoData(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist. try to create table " + tableName);
                createSchemaTables(config);
            }
        }

        insertPhone(config, "北京", "北京", "北京移动全球通卡", "18810314997");
        insertPhone(config, "重庆", "重庆", "联通186卡", "18623691591");
        insertPhone(config, "四川", "绵阳", "四川电信CDMA卡", "18080241234");
    }

    public static void getPhone(Configuration config, String phone) throws IOException {
        try ( Connection connection = ConnectionFactory.createConnection(config))
        {
            Table table =  connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(phone.getBytes());
            Result result = table.get(get);
            for (Cell cell : result.rawCells())
            {
                System.out.println("--------------------" + new String(CellUtil.cloneRow(cell)) + "----------------------------");
                System.out.println("Column Family: " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("Column       :" + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value        : " + new String(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        //config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        //config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        config.addResource("./hbase/hbase-site.xml");
        config.addResource("./hadoop/core-site.xml");

        try {
            DemoData(config);
            getPhone(config, "18080241234");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
