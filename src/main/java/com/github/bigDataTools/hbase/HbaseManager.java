package com.github.bigDataTools.hbase;

import com.github.bigDataTools.util.RunnerThreadPoolSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
          //  HbaseManager.close();
        }
        return false;
    }



    public  boolean insertMultiNew(Table table, String rowKey, String family, Map<String,Object> event) {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            Iterator<Map.Entry<String,Object>> iter = event.entrySet().iterator();
            while (iter.hasNext()){
                Map.Entry<String,Object> map=iter.next();
                String qualifier = map.getKey();
                String value = String.valueOf(map.getValue());
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
            table.put(put);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            //  HbaseManager.close();
        }
        return false;
    }

    public  List<Put> insertMultis(List<Put> list, String rowKey, String family, Map<String,Object> event) {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            Iterator<Map.Entry<String,Object>> iter = event.entrySet().iterator();
            while (iter.hasNext()){
                Map.Entry<String,Object> map=iter.next();
                String qualifier = map.getKey();
                String value = String.valueOf(map.getValue());
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
            list.add(put);
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }


    public  boolean insertList(List<Put> list, String tableName) {
        boolean flag = true;
        try {
            Table t = getConn().getTable(TableName.valueOf(tableName));
            t.put(list);
        } catch (Exception e) {
            flag = false;
            logger.error(e.getMessage());
        } finally {
              HbaseManager.close();
        }
        return flag;
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

    public static long put(String tablename, List<Put> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn =getConn();
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
            close();
        }
        return System.currentTimeMillis() - currentTime;
    }

    public static long putByHTable(String tablename, List<?> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConn();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(tablename));
        htable.setAutoFlushTo(false);
        htable.setWriteBufferSize(5 * 1024 * 1024);
        try {
            htable.put((List<Put>)puts);
            htable.flushCommits();
        } finally {
            htable.close();
            close();
        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * 多线程同步提交
     * @param tableName  表名称
     * @param puts  待提交参数
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     */
    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {
        RunnerThreadPoolSupport.getExecutorService().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    long costTime = put(tableName, puts);
                    System.out.println("costTime = "+costTime);
                } catch (Exception e) {
                    logger.error("batchPut failed . ", e);
                }
            }
        });
        if(waiting){
            try {
                ((ThreadPoolExecutor) RunnerThreadPoolSupport.getExecutorService()).awaitTermination(3000, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("HBase put job thread pool await termination time out.", e);
            }
        }
    }

    /**
     * 多线程异步提交
     * @param tableName  表名称
     * @param puts  待提交参数
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     */
    public void batchAsyncPut(final String tableName, final List<Put> puts, boolean waiting) {
        Future f = RunnerThreadPoolSupport.getExecutorService().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    putByHTable(tableName, puts);
                } catch (Exception e) {
                    logger.error("batchPut failed . ", e);
                }
            }
        });

        if(waiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            } catch (ExecutionException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            }
        }
    }

}

