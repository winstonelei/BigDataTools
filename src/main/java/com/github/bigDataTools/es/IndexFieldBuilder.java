package com.github.bigDataTools.es;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexFieldBuilder {
	
	private final Logger log = LoggerFactory.getLogger(IndexFieldBuilder.class); 

	/**
	 * 索引字段构建
	 * @param fieldType 字段类型
	 * @param fieldName	字段名称
	 * @throws Exception
	 */
	public JSONObject buildFieldMapping(String fieldType, String fieldName) throws Exception {
		JSONObject jo = null;
		try {
			// 字段属性
			jo = new JSONObject();
			String typeName = Class.forName(fieldType).getName();
			// int类型
			if (typeName.equals(IndexMappingType.INTTYPE)) {
				jo.put("type", IndexMappingType.MAPPINGS
						.get(IndexMappingType.INTTYPE));
				jo.put("store", "no");
			}else
			// String类型
			if (typeName.equals(IndexMappingType.STRINGTYPE)) {
				jo.put("type", IndexMappingType.MAPPINGS
						.get(IndexMappingType.STRINGTYPE));
				jo.put("store", "no");
			}else
			// float类型
			if (typeName.equals(IndexMappingType.FLOATTYPE)) {
				jo.put("type", IndexMappingType.MAPPINGS
						.get(IndexMappingType.FLOATTYPE));
				jo.put("store", "no");
			}else
			// double类型
			if (typeName.equals(IndexMappingType.DOUBLETYPE)) {
				jo.put("type", IndexMappingType.MAPPINGS
						.get(IndexMappingType.DOUBLETYPE));
				jo.put("store", "no");
			}else
			// 时间类型
			if (typeName.equals(IndexMappingType.TIMETYPE)) {
				jo.put("type", IndexMappingType.MAPPINGS
						.get(IndexMappingType.TIMETYPE));
				jo.put("store", "no");
			}else{
				/**
				 * 匹配不到，全部为string类型
				 */
				jo.put("type", "string");
				jo.put("store", "no");
			}

		} catch (ClassNotFoundException e) {
			log.error("类型无法找到:" + fieldType, e);
			throw e;
		} catch (NumberFormatException e) {
			log.error("类型转换异常", e);
			throw e;
		} catch (JSONException e) {
			throw e;
		}
		return jo;

	}

}
