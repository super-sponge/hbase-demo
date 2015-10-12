package com.hlbigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by sponge on 15-10-12.
 */
public class WebAPIServer {
    private static final String PHONE_TABLE = "phone";
    private static final String WEATHER_TABLE = "weatherjson";


    public static void getPhoneAddress(Configuration config, String phoneNumber) throws IOException {
        try(Connection conn = ConnectionFactory.createConnection(config)){
            Table table = conn.getTable(TableName.valueOf(PHONE_TABLE));
            String key = phoneNumber;
            if(phoneNumber.length() > 7) {
                key = phoneNumber.substring(0, 7);
            }
            Get get = new Get(key.getBytes());
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("--------------------" + new String(CellUtil.cloneRow(cell)) + "----------------------------");
                System.out.println("Column Family: " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("Column       :" + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value        : " + new String(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void getWeather(Configuration config, String city) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf(WEATHER_TABLE));
            Get get = new Get(city.getBytes("UTF-8"));
            //just get def column family
            get.addFamily("def".getBytes());
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("--------------------" + new String(CellUtil.cloneRow(cell)) + "----------------------------");
                System.out.println("Column Family: " + new String(CellUtil.cloneFamily(cell)));
                System.out.println("Column       :" + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value        : " + new String(CellUtil.cloneValue(cell)));
            }
        }

    }

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)

        String configDir = System.getenv("APP_CONF_DIR");
        System.out.println(configDir);
        config.addResource(new Path(configDir, "hbase-site.xml"));
        config.addResource(new Path(configDir, "core-site.xml"));

        getPhoneAddress(config, "18996192345");
        getWeather(config, "重庆;20151011");

    }
}
