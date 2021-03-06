package com.example.hbase.admin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * Created by sponge on 15-9-29.
 */
public class Example {

    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.SNAPPY));

            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }

    }

    public static void insertData(Configuration config) throws IOException {
        try( Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            Table table = connection.getTable(tableName);
            Put put = new Put("first".getBytes());
            put.addColumn(CF_DEFAULT.getBytes(), "col1".getBytes(), "value1".getBytes());

            System.out.println("Insert Data .");
            table.put(put);

            table.close();

        }
    }

    public static void getData(Configuration config) throws IOException {
        try ( Connection connection = ConnectionFactory.createConnection(config))
        {
            Table table =  connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get("first".getBytes());
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

    public static void modifySchema (Configuration config) throws IOException {
        try (
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin())
        {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            HTableDescriptor table = new HTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
            admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
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
            createSchemaTables(config);
            insertData(config);
            getData(config);
            //modifySchema(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
