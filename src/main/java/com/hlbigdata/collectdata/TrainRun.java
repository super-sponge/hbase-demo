package com.hlbigdata.collectdata;

import org.apache.commons.collections.KeyValue;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sponge on 15-10-21.
 */
public class TrainRun {

    private static final  String APIKEY = "5e708175fca443ba69b6d12f3f87483e";
    private static final String TABLE_NAME = "traininfo";
    private static final String CF_JSON= "cf";

    private static Map<String, String> mapStationCode = new HashMap<>();
    private static List<String> lstStation=new ArrayList<>();

    /**
     * get train info from apix web
     * @param from  start station code
     * @param to    destinatioin station code
     * @param date  query date formated like '2015-10-22'
     * @return      json data
     */
    private static String getFromAPIX(String from, String to, String date) {
        String httpUrl = "http://a.apix.cn/apixlife/ticket/rest";
        String httpArg = "from="+from+"&to="+to+"&date="+date;

        return Driver.requestAPIX(httpUrl,httpArg,APIKEY);
    }

    /**
     *  init station code map
     *
     *  @param file
     *          station code file path
     */
    private static void initStationCode(String file){
        File fs = new File(file);
        BufferedReader reader = null;
        try {
            String stationName = null;
            String stationCode = null;
            String line = null;
            String[] splitLine = null;
            reader = new BufferedReader(new FileReader(fs));
            while((line = reader.readLine()) != null) {
                splitLine = line.split("\t");
                if (splitLine.length !=2) {
                    continue;
                }
                stationName = splitLine[0];
                stationCode = splitLine[1];
                mapStationCode.put(stationName, stationCode);
                lstStation.add(stationName);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader == null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

    }

    public static List<KeyValue> getAllTrainInfo(Configuration config, String file, String date) {
        initStationCode(file);
        List<KeyValue> result = new ArrayList<>();
        String strJson = null;
        try (Connection connect = ConnectionFactory.createConnection(config);
            Admin admin = connect.getAdmin()){
            TableName tablename = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tablename)) {
                System.out.println("Table does not exist.");
                return null;
            }

            Table table = connect.getTable(tablename);
            Put     put = null;
            for(String start : lstStation) {
                for(String end : lstStation) {
                    if (start.equals(end)) {
                        continue;
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    strJson = getFromAPIX(mapStationCode.get(start), mapStationCode.get(end), date );
                    JSONObject jo = new JSONObject(strJson);

                    if (jo.has("httpstatus") && (jo.getInt("httpstatus") == 200)) {
                        JSONObject joData = jo.getJSONObject("data");
                        if (joData.has("flag") && joData.getBoolean("flag")) {
                            result.add(new DefaultKeyValue(start, end));
                            // 插入到hbase
                            String rowkey = start + ":" + end;
                            put = new Put(rowkey.getBytes());
                            put.addColumn(CF_JSON.getBytes(), "json".getBytes(), joData.toString().getBytes("utf-8"));
                            table.put(put);

                            System.out.println("start " + start + "\t end " + end + "\t has ticket");
                        }
                    }
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void run(Configuration config, String file, String date) {
        List<KeyValue>  lsttrain = getAllTrainInfo(config,file,date);
        for(KeyValue item : lsttrain) {
            System.out.println("from " + item.getKey() + " to " + item.getValue());
        }

    }
}
