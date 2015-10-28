package com.hlbigdata.collectdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sponge on 15-10-20.
 */
public class Driver {

    /**
     *
     * @param httpUrl   api url
     * @param httpArg   request args
     * @param APIKEY    APIKEY, should apply from web
     * @return  json data
     */
    public static String requestAPIX(String httpUrl, String httpArg, String APIKEY) throws IOException {
        BufferedReader reader = null;
        String result = null;
        StringBuffer sbf = new StringBuffer();
        httpUrl = httpUrl + "?" + httpArg;

        URL url = new URL(httpUrl);
        HttpURLConnection connection = (HttpURLConnection) url
                .openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("accept", "application/json");
        connection.setRequestProperty("content-type", "application/json");
        // 填入apixkey到HTTP header
        connection.setRequestProperty("apix-key", APIKEY);
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

        return result;
    }

     public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)

        String configDir = System.getenv("APP_CONF_DIR");
        System.out.println(configDir);
        config.addResource(new Path(configDir, "hbase-site.xml"));
        config.addResource(new Path(configDir, "core-site.xml"));

        if (args.length == 1 ) {
            String type = args[0];
            System.out.println(type);
            if ( type.equals("weather")) {
                Weather.getAllCityInfoJson(configDir + "/city_detaill.txt", config);
            } else if (type.equals("pm25")) {
                PM25.getAllPM25(configDir + "/city.txt", config);
            } else {
                System.out.println("Please input weather or pm25");
            }
        } else if (args.length == 2) {
            String type = args[0];
            if (type.equals("train")) {
                TrainRun.run(config, configDir + "/station_code.txt", args[1]);
            }
        }

    }
}