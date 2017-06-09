package com.github.bigDataTools.es;

import java.util.HashMap;
import java.util.Map;

/**
 * 索引映射类型
 * @author winstone
 *
 */
public class IndexMappingType {
	
	public static final Map<String,String> MAPPINGS = new HashMap<String,String>();
	
	static {
		MAPPINGS.put("java.lang.String", "string");
		MAPPINGS.put("java.lang.Integer", "integer");
		MAPPINGS.put("java.lang.Float", "float");
		MAPPINGS.put("java.lang.Double", "double");
		MAPPINGS.put("java.lang.Boolean", "boolean");
		MAPPINGS.put("java.lang.Character", "char");
		MAPPINGS.put("java.util.Date", "date");
	}
	
	public static final String STRINGTYPE = "java.lang.String";
	
	public static final String INTTYPE = "java.lang.Integer";
	
	public static final String FLOATTYPE = "java.lang.Float";
	
	public static final String DOUBLETYPE = "java.lang.Double";

	public static final String BOOLEANTYPE = "java.lang.Boolean";
	
	public static final String TIMETYPE = "java.util.Date";
	
	public static final String CHARTYPE = "java.lang.Character";
	
	
	
}
