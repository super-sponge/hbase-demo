package com.hlbigdata.collectdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.json.JSONObject;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sponge on 15-10-20.
 */
public class Weather {
    private static final String APIX_APIKEY="e1ee7045cf7a4f0462768d9654d7a933";

    private static final String TABLE_NAME = "weatherjson";
    private static final String CF_JSON= "cf";
    private static final String CF_DETAILL = "def";

    private static final String COL_WIND_POWER = "wind_power";
    private static final String COL_POSTCODE = "post_code";
    private static final String COL_HIGHTEMP = "high_temp";
    private static final String COL_TEMP = "temp";
    private static final String COL_TIME = "time";
    private static final String COL_WEATHER = "weather";
    private static final String COL_LOWTEMP = "low_temp";
    private static final String COL_DIRECTION = "wind_direction";
    private static final String COL_CITYCODE = "city_code";


    /**
     * Get all city from web and insert into hbase
     *
     * @param fileName citycode file
     * @param config hbase configuration
     * @throws Exception when something goes wrong
     *
     */
    public static void getAllCityInfoJson(String fileName, Configuration config) {
        // SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String strDate = df.format(new Date());

        System.out.println(strDate + "\tweather\t" + "begin\tallcity\t" + new Date());
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
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
                while ((tempString = reader.readLine()) != null) {
                    String cityId = tempString.split("\t")[0];
                    String cityPing = tempString.split("\t")[1];
                    String cityName = tempString.split("\t")[2];
                    //System.out.println("line " + iter++  + " : " + cityId + " " + cityPing + " " + cityName);
                    String jsonResult = getWeatherFromApi(cityId);
                    String rowKey = cityName + ";" + strDate;
                    Put put = new Put(rowKey.getBytes());
                    put.addColumn(CF_JSON.getBytes(), "json".getBytes(), jsonResult.getBytes());
                    put.addColumn(CF_DETAILL.getBytes(), "cityid".getBytes(), cityId.getBytes());

                    JSONObject jo = new JSONObject(jsonResult);

                    //check return value
                    if (jo.getInt("error_code") == 0) {
                        JSONObject joData = jo.getJSONObject("data");
                        put.addColumn(CF_DETAILL.getBytes(), COL_WIND_POWER.getBytes(), joData.getString(COL_WIND_POWER).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_POSTCODE.getBytes(), joData.getString(COL_POSTCODE).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_HIGHTEMP.getBytes(), joData.getString(COL_HIGHTEMP).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_TEMP.getBytes(), joData.getString(COL_TEMP).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_TIME.getBytes(), joData.getString(COL_TIME).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_WEATHER.getBytes(), joData.getString(COL_WEATHER).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_LOWTEMP.getBytes(), joData.getString(COL_LOWTEMP).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_DIRECTION.getBytes(), joData.getString(COL_DIRECTION).getBytes("utf-8"));
                        put.addColumn(CF_DETAILL.getBytes(), COL_CITYCODE.getBytes(), joData.getString(COL_CITYCODE).getBytes("utf-8"));

                    }

                    table.put(put);
                }
                table.close();
            }
            reader.close();
            strDate = df.format(new Date());
            System.out.println(strDate + "\tweather\t" + "succed\tallcity\t" + new Date());
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


    /**
     * get city info from web
     *
     * @param cityid cityid
     * @return  json weather data
     */
    private static String getWeatherFromApi(String cityid) {
        String httpUrl = "http://a.apix.cn/apixlife/weather/weather";
        String httpArg = "cityid=" + cityid;
        return Driver.requestAPIX(httpUrl, httpArg, APIX_APIKEY);

    }

    private static void ParseJsonWeather(String cityid) {
        String jsonData = getWeatherFromApi(cityid);
        JSONObject jo = new JSONObject(jsonData);
        JSONObject joData = jo.getJSONObject("data");

        System.out.println(joData);
        System.out.println("time " +  joData.getString("time"));

    }

    private static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_JSON).setCompressionType(Compression.Algorithm.SNAPPY));
            table.addFamily(new HColumnDescriptor(CF_DETAILL).setCompressionType(Compression.Algorithm.SNAPPY));

            System.out.print("Creating table. ");

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
            System.out.println(" Done.");
        }
    }


}
