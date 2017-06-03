package com.github.bigDataTools.es;

import java.util.HashMap;
import java.util.Map;

/**
 * 索引映射类型
 */
public class IndexMappingType {
	
	public static final Map<String,String> MAPPINGVALUES = new HashMap<String,String>();
	
	static {
		MAPPINGVALUES.put("java.lang.String", "string");
		MAPPINGVALUES.put("java.lang.Integer", "integer");
		MAPPINGVALUES.put("java.lang.Float", "float");
		MAPPINGVALUES.put("java.lang.Double", "double");
		MAPPINGVALUES.put("java.lang.Boolean", "boolean");
		MAPPINGVALUES.put("java.lang.Character", "char");
		MAPPINGVALUES.put("java.util.Date", "date");
	}
	
	public static final String STRINGTYPE = "java.lang.String";
	
	public static final String INTTYPE = "java.lang.Integer";
	
	public static final String FLOATTYPE = "java.lang.Float";
	
	public static final String DOUBLETYPE = "java.lang.Double";

	public static final String BOOLEANTYPE = "java.lang.Boolean";
	
	public static final String TIMETYPE = "java.util.Date";
	
	public static final String CHARTYPE = "java.lang.Character";
	
	
	
}
