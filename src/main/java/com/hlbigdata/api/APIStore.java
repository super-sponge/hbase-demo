package com.hlbigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sponge on 15-10-9.
 */
public class APIStore {


    public static final String APIX_APIKEY="e1ee7045cf7a4f0462768d9654d7a933";

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
     * get data from hbase
     *
     * @param config hbase configuration
     * @param city  city+day  阿城;20151009 stands for  the weather of 阿城 on 2015-10-09
     * @throws IOException
     */

    public static void getWeatheFromHbase(Configuration config, String city) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(city.getBytes("UTF-8"));
            Result result = table.get(get);
//            for (Cell cell : result.rawCells()) {
//                System.out.println("--------------------" + new String(CellUtil.cloneRow(cell)) + "----------------------------");
//                System.out.println("Column Family: " + new String(CellUtil.cloneFamily(cell)));
//                System.out.println("Column       :" + new String(CellUtil.cloneQualifier(cell)));
//                System.out.println("value        : " + new String(CellUtil.cloneValue(cell)));
//            }

            Cell cell = result.getColumnLatestCell(CF_JSON.getBytes(), "json".getBytes());

            String json = new String(CellUtil.cloneValue(cell), "utf-8");
            JSONObject jo = new JSONObject(json);
            JSONObject joData = jo.getJSONObject("data");
            System.out.println(joData.toString());
            System.out.println("time " +  joData.getString("time"));
        }
    }


    /**
     * Get all city from web and insert into hbase
     *
     * @param fileName citycode file
     * @param config hbase configuration
     * @throws Exception when something goes wrong
     *
     */
    private static void getAllCityInfoJson(String fileName, Configuration config) {
       // SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String strDate = df.format(new Date());

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
                    System.out.println("line " + iter++  + " : " + cityId + " " + cityPing + " " + cityName);
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


                    System.out.println("Insert Data .");
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

    /**
     *
     * @param httpUrl   api url
     * @param httpArg   request args
     * @param APIKEY    APIKEY, should apply from web
     * @return  json data
     */
    public static String request(String httpUrl, String httpArg, String APIKEY) {
        BufferedReader reader = null;
        String result = null;
        StringBuffer sbf = new StringBuffer();
        httpUrl = httpUrl + "?" + httpArg;

        try {
            URL url = new URL(httpUrl);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("accept",  "application/json");
            connection.setRequestProperty("content-type",  "application/json");
            // 填入apixkey到HTTP header
            connection.setRequestProperty("apix-key",  APIKEY);
            connection.connect();
            InputStream is = connection.getInputStream();
            reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String strRead = null;
            while ((strRead = reader.readLine()) != null) {
                sbf.append(strRead);
                sbf.append("\r\n");
            }
            reader.close();
            result = sbf.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
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
        return request(httpUrl, httpArg, APIX_APIKEY);

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

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)

        String configDir = System.getenv("APP_CONF_DIR");
        System.out.println(configDir);
        config.addResource(new Path(configDir, "hbase-site.xml"));
        config.addResource(new Path(configDir, "core-site.xml"));

        getAllCityInfoJson(configDir + "/code.txt", config);

        //getWeatheFromHbase(config, "阿城;20151009");
        //ParseJsonWeather("101021300");
    }
}
