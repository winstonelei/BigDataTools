package com.github.bigDataTools.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.ResourceBundle;

/**
 * Created by winstone on 2016/11/18.
 */
public class HiveTableManager implements  TableManager {

    private static final Log logger = LogFactory.getLog(HiveTableManager.class);

    private SparkJdbcUtil util = null;

    private CheckTableUtilsParams checkUtils;

    private static HiveTableManager hiveTableManager;

    private HiveTableManager()throws Exception{
        util = SparkJdbcUtil.getSparkJdbcUtil().getInstance();
        ResourceBundle rb = ResourceBundle.getBundle("spark");
        util.setUserName(rb.getString("spark.thrift.username"));
        util.setUrl(rb.getString("spark.thrift.ip"),rb.getString("spark.thrift.port"));
        checkUtils = CheckTableUtilsParams.getInstance();
    }

    public static HiveTableManager getInstance()throws Exception{
        if (hiveTableManager == null){
            synchronized (HiveTableManager.class){
                hiveTableManager = new HiveTableManager();
            }
        }
        return hiveTableManager;
    }

    @Override
    public boolean createExternalTable(TableInfo tableInfo) throws Exception {

        boolean flag = false;

        if (!checkUtils.checkHiveTableParams(tableInfo)) {
            logger.error("表参数校验失败");
            return flag;
        }
        //表所处数据库名
        String database = tableInfo.getDataBase();
        try {
            //如果数据库名存在，检查数据库是否存在，不存在创建
            if (StringUtils.isNotEmpty(database)) {
                util.excuteSql("create database if not exists " + database);
            }
            //构建建表语句
            String tablesql = HiveTableStruct.getTableSql(tableInfo);

            logger.info("创建表：" + tableInfo.getTableName());
            flag = util.excuteSql(tablesql);

        } catch (SQLException ex) {
            logger.error("执行sql失败", ex);
            throw ex;
        } catch (ClassNotFoundException ex) {
            logger.error("无法加载类", ex);
            throw ex;
        }
        return flag;

    }

    public boolean createInnerTable(TableInfo tableInfo) throws Exception {

        boolean flag = false;

        if (!checkUtils.checkHiveTableParams(tableInfo)) {
            logger.error("表参数校验失败");
            return flag;
        }
        //表所处数据库名
        String database = tableInfo.getDataBase();
        try {
            //如果数据库名存在，检查数据库是否存在，不存在创建
            if (StringUtils.isNotEmpty(database)) {
                util.excuteSql("create database if not exists " + database);
            }
            //构建建表语句
            String tablesql = HiveTableStruct.getTableSql(tableInfo);

            String dataLoaction = tableInfo.getDataLocation();

            logger.info("创建表：" + tableInfo.getTableName());
            flag = util.excuteSql(tablesql);

            //文件路径为不为空，加载数据
            if (flag && StringUtils.isNotBlank(dataLoaction)) {
                logger.info("加载数据到表：" + tableInfo.getTableName());
                loadDataToTable(dataLoaction, tableInfo.getTableName(), database);
            }
        } catch (SQLException ex) {
            logger.error("执行sql失败", ex);
            throw ex;
        } catch (ClassNotFoundException ex) {
            logger.error("无法加载类", ex);
            throw ex;
        }
        return flag;

    }

    @Override
    public boolean deleteTable(TableInfo tableInfo) throws Exception {
        boolean flag = false;
        String tableName = tableInfo.getTableName();
        String dataBase = tableInfo.getDataBase();
        String deleteSql = HiveTableStruct.getDeleteSql(dataBase, tableName);
        if(StringUtils.isNotBlank(deleteSql)){
            flag = util.excuteSql(deleteSql);
        }
        return flag;
    }

    @Override
    public boolean alterTable(TableInfo tableInfo) {
        return false;
    }

    @Override
    public boolean insert(String dataBase, String tableName, String formatStr) {
        HiveTableStruct.insertSql(dataBase, tableName, formatStr);
        return false;
    }

    /**
     * 加载数据到对应的数据库表中
     * @param dataLoaction 文件路径
     * @param tableName 表名
     * @param database 数据库名
     * @throws Exception
     */
    public boolean loadDataToTable(String dataLoaction, String tableName, String database) throws Exception {
        boolean flag = false;
        String loadSql = HiveTableStruct.getLoaderSql(dataLoaction, tableName, database);
        if (StringUtils.isNotBlank(loadSql)) {
            flag = util.excuteSql(loadSql);
        }
        return flag;
    }
}
