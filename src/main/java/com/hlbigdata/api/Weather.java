package com.hlbigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * Created by sponge on 15-10-9.
 */
public class Weather {
    private static final String TABLE_NAME = "weather";
    private static final String CF_NAME = "cf";
    private static final byte[] COL_WIND_POWER = "wind_power".getBytes();
    private static final byte[] COL_POSTCODE = "post_code".getBytes();
    private static final byte[] COL_HIGHTEMP = "high_temp".getBytes();
    private static final byte[] COL_TEMP = "temp".getBytes();
    private static final byte[] COL_TIME = "time".getBytes();
    private static final byte[] COL_WEATHER = "weather".getBytes();
    private static final byte[] COL_LOWTEMP = "low_temp".getBytes();
    private static final byte[] COL_DIRECTION = "wind_direction".getBytes();
    private static final byte[] COL_CITYCODE = "city_code".getBytes();

    private static void insertWeather(Configuration config,
                                      String city, String windPower,
                                      String postCode, String highTemp,
                                      String temp, String time,
                                      String weather, String lowTemp,
                                      String windDirection, String cityCode) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            Table table = connection.getTable(tableName);
            Put put = new Put(city.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_WIND_POWER, windPower.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_POSTCODE, postCode.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_HIGHTEMP, highTemp.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_TEMP, temp.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_TIME, time.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_WEATHER, weather.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_LOWTEMP, lowTemp.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_DIRECTION, windDirection.getBytes("UTF-8"));
            put.addColumn(CF_NAME.getBytes(), COL_CITYCODE, cityCode.getBytes("UTF-8"));
            System.out.println("Insert Data .");
            table.put(put);

            table.close();
        }
    }

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

    public static void DemoData(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist. try to create table " + tableName);
                createSchemaTables(config);
            }
        }

        insertWeather(config, "北京", "4-5级(17~25m/h)", "100000", "20", "20", "15-10-09 08:00", "晴", "8", "北风", "101010100");

    }

    public static void getWeather(Configuration config, String city) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(city.getBytes("UTF-8"));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
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
            getWeather(config, "北京");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

