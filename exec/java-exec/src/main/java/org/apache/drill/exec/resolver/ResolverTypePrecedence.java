package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.Map;

public class ResolverTypePrecedence {
	

public static final Map<String, Integer> precedenceMap;
    
    static {
   	 precedenceMap = new HashMap<String, Integer>();
   	 precedenceMap.put("TINYINT", 1);
   	 precedenceMap.put("SMALLINT", 2);
   	 precedenceMap.put("INT", 3);
   	 precedenceMap.put("BIGINT", 4);
   	 precedenceMap.put("FLOAT4", 5);
   	 precedenceMap.put("FLOAT8", 6);
   	 precedenceMap.put("VARCHAR", 7);
   	 
    }


}
