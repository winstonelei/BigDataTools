package com.github.bigDataTools.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by winstone on 2016/12/9.
 */
public class HbaseManager {

    private static final Log logger = LogFactory.getLog(HbaseManager.class);

    private static  HbaseManager manager;

    private static Connection conn;

    private static Configuration conf;

    private static final String ZKCONSTR = "hbase.zookeeper.quorum";

    private static final String HBASEDIR = "hbase.rootdir";


    private HbaseManager(){
         conf = HBaseConfiguration.create();
         ResourceBundle rb = ResourceBundle.getBundle("commons");
         conf.set("hbase.zookeeper.quorum", rb.getString(ZKCONSTR));
         conf.set("hbase.rootdir", rb.getString(HBASEDIR));
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static HbaseManager getInstance(){
        if(null == manager ){
            synchronized (HbaseManager.class){
                manager = new HbaseManager();
            }
        }
        return  manager;
    }

    /**
     * 获取连接
     * @return
     */
    public static Connection getConn(){
        if (conn == null || conn.isClosed()) {
            try {
                if(conf==null){
                    conf = HBaseConfiguration.create();
                    ResourceBundle rb = ResourceBundle.getBundle("commons");
                    conf.set("hbase.zookeeper.quorum", rb.getString(ZKCONSTR));
                    conf.set("hbase.rootdir", rb.getString(HBASEDIR));
                }
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return conn;
    }

    public static void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * 创建普通表
     * @param tableName
     * @param familyColumn
     * @throws Exception
     */
    public void createTable(String tableName, String...familyColumn) throws Exception {
        TableName table = TableName.valueOf(tableName);
        try {
            Admin admin = getConn().getAdmin();
            boolean isExists = admin.tableExists(table);
            if(isExists){
                return;
            }else{
                HTableDescriptor htd = new HTableDescriptor(table);
                for (String fc : familyColumn) {
                    HColumnDescriptor hcd = new HColumnDescriptor(fc);
                    htd.addFamily(hcd);
                }
                admin.createTable(htd);
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表，并且创建好预分区
     * @param tableName
     * @param familyColumn
     * @throws Exception
     */
    public void createTableWithSplits(String tableName, String...familyColumn) throws Exception {
        TableName table = TableName.valueOf(tableName);
        try {
            Admin admin = getConn().getAdmin();
            boolean isExists = admin.tableExists(table);
            if(isExists){
                return;
            }else{
                HashChoreWoker worker = new HashChoreWoker(1000000, 10);
                byte[][] splitKeys = worker.calcSplitKeys();
                HTableDescriptor htd = new HTableDescriptor(table);
                for (String fc : familyColumn) {
                    HColumnDescriptor hcd = new HColumnDescriptor(fc);
                    htd.addFamily(hcd);
                }
                admin.createTable(htd,splitKeys);
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建表并且自动创建协处理器
     * @param tableName
     * @param oberverName
     * @param path
     * @param map
     * @param familyColumn
     * @throws Exception
     */
    public void createTableWithCoprocessor(String tableName,String oberverName,String path,Map<String,String> map, String...familyColumn) throws Exception {
        TableName table = TableName.valueOf(tableName);
        Admin admin = getConn().getAdmin();
        boolean isExists = admin.tableExists(table);
        if(isExists){
            return ;
        }else{
            try {
                HTableDescriptor htd = new HTableDescriptor(table);
                for (String fc : familyColumn) {
                    HColumnDescriptor hcd = new HColumnDescriptor(fc);
                    htd.addFamily(hcd);
                }
                admin.createTable(htd);
                admin.disableTable(table);
                HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
                for (String fc : familyColumn) {
                    HColumnDescriptor hcd = new HColumnDescriptor(fc);
                    hTableDescriptor.addFamily(hcd);
                }
                hTableDescriptor.addCoprocessor(oberverName, new Path(path), Coprocessor.PRIORITY_USER, map);
                admin.modifyTable(table, hTableDescriptor);
                admin.enableTable(table);
                admin.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public  void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = conn.getAdmin();
            admin.disableTable(tn);
            admin.deleteTable(tn);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  boolean insert(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            t.put(put);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            HbaseManager.close();
        }
        return false;
    }


    public  boolean insertMulti(String tableName, String rowKey, String family, Map<String,Object> event) {
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            Iterator<Map.Entry<String,Object>> iter = event.entrySet().iterator();
            while (iter.hasNext()){
                Map.Entry<String,Object> map=iter.next();
                String qualifier = map.getKey();
                String value = String.valueOf(map.getValue());
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
            t.put(put);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            HbaseManager.close();
        }
        return false;
    }


    public  boolean del(String tableName, String rowKey, String family, String qualifier) {
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));

            if (qualifier != null) {
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else if (family != null) {
                del.addFamily(Bytes.toBytes(family));
            }
            t.delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HbaseManager.close();
        }
        return false;
    }

    /**
     * 删除一行
     * @param tableName
     * @param rowKey
     * @return
     */
    public  boolean del(String tableName, String rowKey) {
        return del(tableName, rowKey, null, null);
    }

    /**
     * 删除一行中的一列
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     */
    public  boolean del(String tableName, String rowKey, String family) {
        return del(tableName, rowKey, family, null);
    }


    /**
     * 得到hbase 中的某一项的值
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     */
    public  String getColumnValue(String tableName,String rowKey,String family,String qualifier){
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result r = t.get(get);
            return Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * 得到某一个family 的所有字段的值
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     */
    public  Map<String, String> getRowValue(String tableName, String rowKey, String family) {
        Map<String, String> result = null ;
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            result = cs.size() > 0 ? new HashMap<String, String>() : result;
            for (Cell cell : cs) {
                result.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return result;
    }

    /**
     * 得到所有rowkey 所有列族中的所有的值
     * @param tableName
     * @param rowKey
     * @return
     */
    public  Map<String, Map<String, String>> getAllColumnFamilyVal(String tableName, String rowKey) {
        Map<String, Map<String, String>> results = null ;
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            results = cs.size() > 0 ? new HashMap<String, Map<String, String>> () : results;
            for (Cell cell : cs) {
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                if (results.get(familyName) == null)
                {
                    results.put(familyName, new HashMap<String,  String> ());
                }
                results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return results;
    }



}

