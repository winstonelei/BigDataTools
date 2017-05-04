package com.github.bigDataTools.hive;


/**
 * Created by winstone on 2016/11/18.
 */
public interface TableManager {

    /**
     * 创建内部表
     * @param tableInfo
     * @return
     */
    public boolean createInnerTable(TableInfo tableInfo)throws Exception;


    /**
     * 创建外部表
     * @param tableInfo
     * @return
     * @throws Exception
     */
    public boolean createExternalTable(TableInfo tableInfo)throws Exception;

    /**
     * 删除表
     * @return
     */
    public boolean deleteTable(TableInfo tableInfo) throws Exception;

    /**
     * 修改表结构
     * @return
     */
    public boolean alterTable(TableInfo tableInfo);

    /**
     * insert 数据
     */
    public boolean insert(String dataBase, String tableName, String formatStr);

}
