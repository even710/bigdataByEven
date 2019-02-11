package com.even.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * des:
 * author: Even
 * create date:2019/1/29
 */
public class HBaseTest {
    public static Configuration conf;
    public static Connection connection;

    static {
        /*resource文件夹下需要有配置文件*/
        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","hd-even-01,hd-even-02,hd-even-03");
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExitTable(String tableName) throws IOException {
        /*获取管理表*/
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return true;
        } else {
            System.out.println("查询不到表");
            return false;
        }
    }

    /**
     * String...表示可变参数
     *
     * @param tableName
     * @param column_Family
     * @throws IOException
     */
    public static void createTable(String tableName, String... column_Family) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表已存在，请输入其它表名");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String column : column_Family) {
                hTableDescriptor.addFamily(new HColumnDescriptor(column));
            }
            admin.createTable(hTableDescriptor);
            System.out.println("表已创建成功！");
        }
    }

    /**
     * 删除表操作
     *
     * @param tableName
     */
    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (isExitTable(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("删除表成功");
        }
    }

    /**
     * 添加数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (isExitTable(tableName)) {
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            boolean isExit = false;
            /*需要判断一下列族是否存在*/
            for (HColumnDescriptor hColumnDescriptor : columnFamilies) {
                String s = hColumnDescriptor.getNameAsString();
                if (columnFamily.endsWith(s)) {
                    isExit = true;
                    break;
                }
            }
            /*如果列族存在，则put数据*/
            if (isExit) {
                Put p = new Put(Bytes.toBytes(rowKey));
                p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                Table table = connection.getTable(TableName.valueOf(tableName));
                table.put(p);
                System.out.println("数据插入成功！");
            } else
                System.out.println("不存在的列族！");
        }
    }

    /**
     * 删除指定rowkey的数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException {
        if (isExitTable(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            if (table.exists(new Get(Bytes.toBytes(rowKey))))
                table.delete(new Delete(Bytes.toBytes(rowKey)));
            else
                System.out.println("不存在的rowKey数据，请输入正确的rowKey");
        } else
            System.out.println("不存在的表，请输入正确的表名！");
    }

    /**
     * 删除多个rowkey的数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteByRowKeys(String tableName, String... rowKey) throws IOException {
        if (isExitTable(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<>();
            for (String row : rowKey) {
                if (table.exists(new Get(Bytes.toBytes(row))))
                    deletes.add(new Delete(Bytes.toBytes(row)));
            }
            table.delete(deletes);
            System.out.println("删除数据成功!");
        }

    }


    /**
     * 扫描表
     *
     * @param tableName
     * @throws IOException
     */
    public static void scanAll(String tableName) throws IOException {
        if (isExitTable(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner scanner = table.getScanner(new Scan());
            for (Result next : scanner) {
                Cell[] cells = next.rawCells();
                for (Cell c : cells) {
                    System.out.println("行键为：" + Bytes.toString(CellUtil.cloneRow(c)));
                    System.out.println("列族为：" + Bytes.toString(CellUtil.cloneFamily(c)));
                    System.out.println("值为：" + Bytes.toString(CellUtil.cloneValue(c)));
                    System.out.println(c);
                }
            }
        }
    }

    /**
     * 根据rowkey扫描表
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void scanByRowKey(String tableName, String rowKey) throws IOException {
        if (isExitTable(tableName)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes("info"));
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));

            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell c : cells) {
                System.out.println(c);
                System.out.println("行键为：" + Bytes.toString(CellUtil.cloneRow(c)));
                System.out.println("列族为：" + Bytes.toString(CellUtil.cloneFamily(c)));
                System.out.println("值为：" + Bytes.toString(CellUtil.cloneValue(c)));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        connect();
//        System.out.println(isExitTable("table"));
//        createTable("even", "info", "info1");
//        deleteTable("even");
        putData("table1", "1002", "info", "name", "elliann");
//        deleteByRowKey("table1", "1003");
//        deleteByRowKeys("table1", "1001", "1002");
        scanAll("table1");
        closeConnection();
    }


    public static void connect() throws IOException {
        connection = ConnectionFactory.createConnection(conf);
    }

    public static void closeConnection() throws IOException {
        connection.close();
    }
}
