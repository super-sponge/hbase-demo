package com.hlbigdata.collectdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sponge on 15-10-20.
 */
public class PM25 {
    private static final String APIX_APIKEY="b1e5d0f03b7644485b1effea13dfc4bf";

    private static final String TABLE_NAME = "pm25";
    private static final String CF_DEF= "cf";

    private static final String COL_CITY = "city";
    private static final String COL_PM25 = "pm25";
    private static final String COL_CLASS = "class";
    private static final String COL_PRIMARY = "primary";

    /**
     * get city info from web
     *
     * @param cityname cityid
     * @return  json city data
     */
    private static String getFromAPIX(String cityname) {
        String httpUrl = "http://a.apix.cn/apixlife/pm25/PM2.5";
        String httpArg = "cityname=" + cityname;
        return Driver.requestAPIX(httpUrl, httpArg, APIX_APIKEY);
    }

    /**
     * Get all city from web and insert into hbase
     *
     * @param fileName citycode file
     * @param config hbase configuration
     * @throws Exception when something goes wrong
     *
     */
    public static void getAllPM25(String fileName, Configuration config) {
        // SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
        String strDate = df.format(new Date());

        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String cityName = null;
            //去掉第一行
            reader.readLine();
            try( Connection connection = ConnectionFactory.createConnection(config);
                 Admin admin = connection.getAdmin()) {
                TableName tableName = TableName.valueOf(TABLE_NAME);
                if(!admin.tableExists(tableName)) {
                    System.out.println("Table does not exist.");
                    System.exit(-1);
                }
                Table table = connection.getTable(tableName);
                int iter = 1;
                while ((cityName = reader.readLine()) != null) {
                    String jsonResult = getFromAPIX(cityName);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String rowKey = cityName + ";" + strDate;
                    Put put = new Put(rowKey.getBytes());
                    put.addColumn(CF_DEF.getBytes(), COL_CITY.getBytes(), cityName.getBytes());

                    JSONObject jo = new JSONObject(jsonResult);

                    JSONObject joData = jo.getJSONObject("data");
                    //check return value
                    if (joData.has("city")) {
                        put.addColumn(CF_DEF.getBytes(), COL_PM25.getBytes(), joData.getString(COL_CITY).getBytes("utf-8"));
                        put.addColumn(CF_DEF.getBytes(), COL_CLASS.getBytes(), joData.getString(COL_CLASS).getBytes("utf-8"));
                        put.addColumn(CF_DEF.getBytes(), COL_PRIMARY.getBytes(), joData.getString(COL_PRIMARY).getBytes("utf-8"));

                        System.out.println(strDate + "\tpm25\t" + "succed\t" + cityName);
                    } else {
                        System.out.println(strDate + "\tpm25\t" + "error\t" + cityName);
                    }
                    table.put(put);
                }
                table.close();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }


}
