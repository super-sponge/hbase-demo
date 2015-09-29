package com.example.hbase.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Created by sponge on 15-9-29.
 */
public class HBaseDemo {

    static Configuration conf = HBaseConfiguration.create();

    static void initConf() {
        //this.conf.addResource();
    }

    public static void createTable(String tablename, String columnFamily) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tablename)) {
            System.out.println("Table exists!");
            System.exit(0);
        }
        else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
        admin.close();

    }

}
