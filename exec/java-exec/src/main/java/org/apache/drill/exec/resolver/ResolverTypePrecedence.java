package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class ResolverTypePrecedence {
	

public static final Map<MinorType, Integer> precedenceMap;

public static final Map<DataMode, Integer> dmPrecedenceMap;

  static {    
    /* The precedenceMap is used to decide whether it's allowed to implicitly "promote" 
     * one type to another type. 
     * 
     * The order that each type is inserted into HASHMAP decides its precedence. 
     * First in ==> lowest precedence. 
     * A type of lower precedence can be implicitly "promoted" to type of higher precedence.
     * For instance, NULL could be promoted to any other type; 
     * tinyint could be promoted into int; but int could NOT be promoted into tinyint (due to possible precision loss).  
     */
    int i = 0;
    precedenceMap = new HashMap<MinorType, Integer>();
   	precedenceMap.put(MinorType.NULL, i++);       // NULL is legal to implicitly be promoted to any other type	 
  	precedenceMap.put(MinorType.FIXEDBINARY, i++); // Fixed-length is promoted to var length
  	precedenceMap.put(MinorType.VARBINARY, i++);
    precedenceMap.put(MinorType.FIXEDCHAR, i++);
   	precedenceMap.put(MinorType.VARCHAR, i++);
    precedenceMap.put(MinorType.FIXED16CHAR, i++);
   	precedenceMap.put(MinorType.VAR16CHAR, i++);
   	precedenceMap.put(MinorType.BIT, i++);
   	precedenceMap.put(MinorType.TINYINT, i++);   //type with few bytes is promoted to type with more bytes ==> no data loss. 
   	precedenceMap.put(MinorType.UINT1, i++);     //signed is legal to implicitly be promoted to unsigned.   
   	precedenceMap.put(MinorType.SMALLINT, i++);
   	precedenceMap.put(MinorType.UINT2, i++); 
  	precedenceMap.put(MinorType.INT, i++);
  	precedenceMap.put(MinorType.UINT4, i++); 
  	precedenceMap.put(MinorType.BIGINT, i++);
  	precedenceMap.put(MinorType.UINT8, i++);
  	precedenceMap.put(MinorType.MONEY, i++);
  	precedenceMap.put(MinorType.DECIMAL4, i++);
  	precedenceMap.put(MinorType.DECIMAL8, i++);
  	precedenceMap.put(MinorType.DECIMAL12, i++);
  	precedenceMap.put(MinorType.DECIMAL16, i++);
  	precedenceMap.put(MinorType.FLOAT4, i++);
  	precedenceMap.put(MinorType.FLOAT8, i++);
  	precedenceMap.put(MinorType.TIME, i++);
  	precedenceMap.put(MinorType.DATE, i++);
  	precedenceMap.put(MinorType.DATETIME, i++);
    precedenceMap.put(MinorType.TIMETZ, i++);
    precedenceMap.put(MinorType.TIMESTAMP, i++);  	
    
    /*
     * Non-nullable (REQUIRED) could be promoted to nullable (OPTIONAL).
     */
    dmPrecedenceMap = new HashMap<DataMode, Integer>();
    int j = 0;
    
    dmPrecedenceMap.put(DataMode.REQUIRED, j++);
    dmPrecedenceMap.put(DataMode.OPTIONAL, j++);
    
  }


}
