package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.Map;

public class ResolverTypePrecedence {
	

public static final Map<String, Integer> precedenceMap;
    
  static {
    
    /* Note: the order that each type is inserted into hashmap determins
     * it's precedence. First in ==> lowest precedence. 
     * A type of lower precedence can be implicitly "promoted" to type of higher precedence 
     */
    
    precedenceMap = new HashMap<String, Integer>();
   	precedenceMap.put("NULLEXPRESSION", precedenceMap.size());	 
  	precedenceMap.put("FIXEDBINARY", precedenceMap.size());
  	precedenceMap.put("VARBINARY", precedenceMap.size());
    precedenceMap.put("FIXEDCHAR", precedenceMap.size());
   	precedenceMap.put("VARCHAR", precedenceMap.size());
    precedenceMap.put("FIXED16CHAR", precedenceMap.size());
   	precedenceMap.put("VAR16CHAR", precedenceMap.size());
   	precedenceMap.put("BIT", precedenceMap.size());
   	precedenceMap.put("TINYINT", precedenceMap.size());
   	precedenceMap.put("SMALLINT", precedenceMap.size());
  	precedenceMap.put("INT", precedenceMap.size());
  	precedenceMap.put("BIGINT", precedenceMap.size());
  	precedenceMap.put("MONEY", precedenceMap.size());
  	precedenceMap.put("DECIMAL4", precedenceMap.size());
  	precedenceMap.put("DECIMAL8", precedenceMap.size());
  	precedenceMap.put("DECIMAL12", precedenceMap.size());
  	precedenceMap.put("DECIMAL16", precedenceMap.size());
  	precedenceMap.put("FLOAT4", precedenceMap.size());
  	precedenceMap.put("FLOAT8", precedenceMap.size());
  	precedenceMap.put("TIME", precedenceMap.size());
  	precedenceMap.put("DATE", precedenceMap.size());
  	precedenceMap.put("DATETIME", precedenceMap.size());
    precedenceMap.put("TIMETZ", precedenceMap.size());
  	
  }


}
