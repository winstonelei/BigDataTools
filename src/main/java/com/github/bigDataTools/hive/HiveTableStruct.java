package com.github.bigDataTools.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by winstone on 2016/11/18.
 */
public class HiveTableStruct {

    private static final Log logger = LogFactory.getLog(HiveTableStruct.class);

    /**
     * 构建hive加载数据语句
     * @param location
     * @param tableName
     * @param database
     * @return
     */
    public static String getLoaderSql(String location, String tableName, String database) {
        StringBuilder loadSql = new StringBuilder(0);
        loadSql.setLength(0);
        loadSql.append("LOAD DATA INPATH  '");
        loadSql.append(location);
        loadSql.append("' INTO TABLE ");
        if (StringUtils.isNotBlank(database)) {
            loadSql.append(database);
            loadSql.append(".");
            loadSql.append(tableName);
        } else {
            loadSql.append(tableName);
        }
        return loadSql.toString();
    }
    /**
     * 构建删除表sql语句
     * @param database
     * @param tableName
     * @return
     */
    public static String getDeleteSql(String database,String tableName){
        StringBuilder sb = new StringBuilder();
        if(StringUtils.isNotBlank(tableName)){
            sb.append("drop table ");
            if(StringUtils.isNotBlank(database)){
                sb.append(database);
                sb.append(".");
                sb.append(tableName);
            }

        }
        return sb.toString();
    }


    /**
     * 构建table sql
     * @param tableInfo
     * @return
     */
    public static String getTableSql(TableInfo tableInfo) {
        StringBuilder sb = new StringBuilder();
        String tableName = tableInfo.getTableName();
        String dataBase = tableInfo.getDataBase();
        sb.append("create  external  table if not exists ");
        if (StringUtils.isNotBlank(dataBase)) {
            sb.append(dataBase);
            sb.append(".");
            sb.append(tableName);
        } else {
            sb.append(tableName);
        }
        List<FieldInfo> fields = tableInfo.getFieldInfos();
        //按索引排序
        Collections.sort(fields);
        sb.append(" ( ");
        int count = 1;
        for (FieldInfo field : fields) {
            if (count == fields.size()) {
                sb.append(field.getFieldName());
                sb.append("    ");
                sb.append(field.getFieldType());

            } else {
                sb.append(field.getFieldName());
                sb.append("    ");
                sb.append(field.getFieldType());
                sb.append(", ");
            }

            count++;
        }
        sb.append(" ) ");
        sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
        sb.append(tableInfo.getFieldSplit());
        sb.append("' ");
        sb.append(" location ");
        sb.append(" '");
        sb.append(tableInfo.getDataLocation());
        sb.append("' ");
        return sb.toString();
    }
    /**
     * insert 数据的语句构建
     * @param dataBase
     * @param tableName
     * @param insertSql
     * @return
     */
    public static String insertSql(String dataBase, String tableName, String insertSql){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT INTO TABLE " + dataBase + "." + tableName
                + " VALUES (" + insertSql + ")");
        return stringBuilder.toString();
    }

}
