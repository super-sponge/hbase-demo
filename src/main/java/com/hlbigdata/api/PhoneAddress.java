package com.hlbigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.json.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sponge on 15-10-9.
 */
public class PhoneAddress {
    private static final String TABLE_NAME = "phone";
    private static final String CF_NAME = "cf";
    private static final byte[] COL_PROVINCE = "province".getBytes();
    private static final byte[] COL_CITY = "city".getBytes();
    private static final byte[] COL_OPERATOR = "operator".getBytes();
    private static final byte[] COL_AREACODE = "areacode".getBytes();

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


    private static Map<String,String> readPhoneCode(String fileName) {


        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            Map<String,String> result=new HashMap<>();
            String tempString = null;
            String[] strSplit = null;

            while ((tempString = reader.readLine()) != null) {
                strSplit = tempString.split("\t");
                if (strSplit.length == 3) {
                    result.put(strSplit[0] + ":" + strSplit[1] + ":" + strSplit[2].substring(0,3), strSplit[2]);
                } else {
                    System.out.println("Error: " + tempString);
                }
            }
            reader.close();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    private static String getPhoneAddressApi(String phoneNumber) {
        String httpUrl = "http://a.apix.cn/apixlife/phone/phone";
        String httpArg = "phone=" + phoneNumber;
        String phoneaddressKey = "9d3fda97c68c4c377bac1c608eeef776";
        return APIStore.request(httpUrl, httpArg, phoneaddressKey);
    }

    private static Map<String, PhoneInfo> transToAddress(Map<String,String> mapData) throws ParseException, InterruptedException {

        Map<String, PhoneInfo> mapResult = new HashMap<>();
        for(Map.Entry<String, String> entry:mapData.entrySet()){
            //System.out.println(entry.getKey()+"--->"+entry.getValue());
            String tempString = null;
            try {
                tempString = getPhoneAddressApi(entry.getValue() + "0000");
            } catch (Exception e){
                Thread.sleep(5000);
                tempString = getPhoneAddressApi(entry.getValue() + "0000");
            }
            Thread.sleep(100);

            JSONObject jo = new JSONObject(tempString);
            if ( jo.getInt("error_code") != 0) {
                System.out.println("ErrorMsg: "  + jo.getString("message"));
                continue;
            }
            JSONObject jsonObj = jo.getJSONObject("data");

            PhoneInfo phone = new PhoneInfo();
            //JsonHelper.toJavaBean(phone, jsonStr);

            phone.setCity(jsonObj.getString("city"));
            phone.setTelephone(jsonObj.getString("telephone"));
            phone.setOperator(jsonObj.getString("operator"));
            phone.setProvince(jsonObj.getString("province"));

            mapResult.put(entry.getKey(), phone);
        }


        return mapResult;
    }

    private static void writeToHbase(Configuration config, String fileName) throws ParseException, InterruptedException {

        Map<String,String> mapData = readPhoneCode(fileName);
        Map<String, PhoneInfo> mapPhone = transToAddress(mapData);

        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName + ".out")));

            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            String[] strSplit = null;

            while ((tempString = reader.readLine()) != null) {
                strSplit = tempString.split("\t");
                if (strSplit.length == 3) {
                    PhoneInfo  phone =  mapPhone.get(strSplit[0] + ":" + strSplit[1] + ":" + strSplit[2].substring(0,3));
                    if(phone != null ) {
                        //insertPhone(config, phone.getProvince(),phone.getCity(),phone.getOperator(), strSplit[2]);
                        writer.write(tempString + "\t" + phone.getProvince() + "\t" + phone.getCity() + "\t" + phone.getOperator() + "\n");
                    } else {
                        System.out.println("can't find the number " + strSplit[2]);
                    }
                } else {
                    System.out.println("Error: " + tempString);
                }
            }

            writer.close();
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

    private static void Process175(String fileOut, String fileCode) {
        File file = new File(fileOut);
        File fileC = new File(fileCode);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            Map<String,String> areaCode=new HashMap<>();
            Map<String,String> OperatorCode = new HashMap<>();
            String tempString = null;
            String[] strSplit = null;

            while ((tempString = reader.readLine()) != null) {
                strSplit = tempString.split("\t");
                if (strSplit.length == 6) {
                    areaCode.put(strSplit[0],strSplit[3] + "\t" + strSplit[4]);
                    OperatorCode.put(strSplit[1], strSplit[5]);

                } else {
                    System.out.println("Error: " + tempString);
                }
            }

            for(Map.Entry<String, String> entry:areaCode.entrySet()) {
                System.out.println(entry.getKey() + "--->" + entry.getValue());
            }
            for(Map.Entry<String, String> entry:OperatorCode.entrySet()) {
                System.out.println(entry.getKey() + "--->" + entry.getValue());
            }
            OperatorCode.put("2", "联通175卡");
            reader.close();
            reader = new BufferedReader(new FileReader(fileC));
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileCode + ".175.out")));
            while ((tempString = reader.readLine()) != null) {
                strSplit = tempString.split("\t");

                if (strSplit.length == 3) {

                    String str175 = strSplit[2].substring(0, 3);

                    //System.out.println(strSplit[2] + ": " + str175);
                    if (str175.equals("175")) {
                        System.out.println(tempString + "\t" + areaCode.get(strSplit[0]) + "\t" + OperatorCode.get(strSplit[1]));
                        writer.write(tempString + "\t" + areaCode.get(strSplit[0]) + "\t" + OperatorCode.get(strSplit[1]) + "\n");

                    }
                }
            }
            writer.close();
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

    private static void writeFileToHbase(Configuration config, String fileName) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist. try to create table " + tableName);
                createSchemaTables(config);
            }

            File file = new File(fileName);
            BufferedReader reader = null;
            try {

                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                String[] strSplit = null;

                Table table = connection.getTable(tableName);
                while ((tempString = reader.readLine()) != null) {
                    strSplit = tempString.split("\t");
                    if (strSplit.length == 6) {


                        Put put = new Put(strSplit[2].getBytes());
                        put.addColumn(CF_NAME.getBytes(), COL_PROVINCE, strSplit[3].getBytes("UTF-8"));
                        put.addColumn(CF_NAME.getBytes(), COL_CITY, strSplit[4].getBytes("UTF-8"));
                        put.addColumn(CF_NAME.getBytes(), COL_OPERATOR, strSplit[5].getBytes("UTF-8"));
                        put.addColumn(CF_NAME.getBytes(), COL_AREACODE, strSplit[0].getBytes("UTF-8"));

                        table.put(put);

                    } else {
                        System.out.println("Error: " + tempString);
                    }
                }
                reader.close();

            table.close();
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

    public static void main(String[] args) throws ParseException, IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)

        String configDir = System.getenv("APP_CONF_DIR");
        System.out.println(configDir);
        config.addResource(new Path(configDir, "hbase-site.xml"));
        config.addResource(new Path(configDir, "core-site.xml"));

        //writeToHbase(config, configDir + "/phone_code.txt" );

        //Process175(configDir + "/phone_code.txt.out", configDir + "/phone_code.txt" );
        writeFileToHbase(config, configDir + "/phone_code_detaill.txt");
        //getPhone(config, "1818601");
    }
}
