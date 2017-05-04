package com.github.bigDataTools.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Created by winstone on 2016/11/18.
 */
public class CheckTableUtilsParams {

    private static final Log logger = LogFactory.getLog(CheckTableUtilsParams.class);

    private static CheckTableUtilsParams instance = new CheckTableUtilsParams();

    private CheckTableUtilsParams(){}

    public static CheckTableUtilsParams getInstance(){
        return instance;
    }

    /**
     * check hive table
     * @param table
     * @return
     */
    public static boolean checkHiveTableParams(TableInfo table)throws TableParamsCheckException{

        boolean flag  = false;
        String tableName = table.getTableName();
        String split = table.getFieldSplit();
        List<FieldInfo> fieldInfoList = table.getFieldInfos();

        if(StringUtils.isBlank(tableName)){
            logger.equals("hive表名称不能为空");
            throw new TableParamsCheckException("hive表名称不能为空");
        }
        if(StringUtils.isBlank(split)){
            logger.equals("hive表分隔符为空");
            throw new TableParamsCheckException("hive表分隔符为空");
        }
        if(fieldInfoList==null || fieldInfoList.isEmpty()){
            logger.error("hive表字段为空");
            throw new TableParamsCheckException("hive表字段为空");
        }
       for(FieldInfo fieldInfo : fieldInfoList){
           if(StringUtils.isBlank(fieldInfo.getFieldName())){
               logger.error("hive表字段名为空");
               throw new TableParamsCheckException("hive表字段名为空");
           }
       }
        flag = true;
        return flag;
    }


}
