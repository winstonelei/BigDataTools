package com.github.bigDataTools.hive;

import java.util.List;

/**
 * Created by winstone on 2016/11/18.
 */
public class TableInfo {

    protected String tableName;                   //表名

    protected String dataBase;                   //属于数据库

    protected String fieldSplit;                    //字段分隔符

    protected List<FieldInfo> fieldInfos;        //字段信息

    private String dataLocation;                //数据文件所在路经

    public String getDataLocation() {
        return dataLocation;
    }

    public void setDataLocation(String dataLocation) {
        this.dataLocation = dataLocation;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public List<FieldInfo> getFieldInfos() {
        return fieldInfos;
    }

    public void setFieldInfos(List<FieldInfo> fieldInfos) {
        this.fieldInfos = fieldInfos;
    }

    public String getFieldSplit() {
        return fieldSplit;
    }

    public void setFieldSplit(String fieldSplit) {
        this.fieldSplit = fieldSplit;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
