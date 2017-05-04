package com.github.bigDataTools.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by winstone on 2016/11/18.
 */
public class FieldInfo implements  Comparable{

    private static final Log logger = LogFactory.getLog(FieldInfo.class);

    private String fieldName;       //字段名称

    private Integer fieldIndex;         //字段索引

    private FieldType fieldType;    //字段类型

    public FieldInfo(){};

    public FieldInfo(String fieldName, Integer fieldIndex){

        this.fieldName = fieldName;
        this.fieldIndex = fieldIndex;
    }

    public FieldInfo(String fieldName, Integer fieldIndex, FieldType fieldType){

        this.fieldName = fieldName;
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getFieldIndex() {
        return fieldIndex;
    }

    public void setFieldIndex(Integer fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    @Override
    public String toString() {
        return "FieldInfo{" + "fieldName=" + fieldName + ", fieldIndex=" + fieldIndex + ", fieldType=" + fieldType + '}';
    }


    public int compareTo(Object o) {
        FieldInfo comparaClass = (FieldInfo) o;
        int diff = this.getFieldIndex() - comparaClass.getFieldIndex();
        if (diff == 0) {
            logger.error("表字段索引存在相同的值,源类："+toString()+",比较类："+comparaClass.toString());
            throw new RuntimeException("表字段索引存在相同的值,源类："+toString()+",比较类："+comparaClass.toString());
        }
        return diff;
    }
}
