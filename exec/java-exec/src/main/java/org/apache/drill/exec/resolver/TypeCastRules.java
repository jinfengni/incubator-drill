package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

public class TypeCastRules {

  private static Map<MinorType, Set<MinorType>> rules;

  public TypeCastRules() {
  }

  static {
    initTypeRules();
  }

  private static void initTypeRules() {
    rules = new HashMap<MinorType, Set<MinorType>>();

    Set<MinorType> rule;

    /** TINYINT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.TINYINT, rule);

    /** SMALLINT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.SMALLINT, rule);

    /** INT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.INT, rule);

    /** BIGINT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.BIGINT, rule);

    /** DECIMAL4 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL4, rule);

    /** DECIMAL8 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL8, rule);

    /** DECIMAL12 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL12, rule);

    /** DECIMAL16 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL16, rule);

    /** MONEY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.MONEY, rule);

    /** DATE cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.DATE);
    rule.add(MinorType.DATETIME);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DATE, rule);

    /** TIME cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TIME);
    rule.add(MinorType.DATETIME);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.TIME, rule);

    /** DATETIME cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.DATETIME);
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rules.put(MinorType.DATETIME, rule);

    /** FLOAT4 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rules.put(MinorType.FLOAT4, rule);

    /** FLOAT8 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rules.put(MinorType.FLOAT8, rule);

    /** BIT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.BIT, rule);

    /** FIXEDCHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.DATETIME);
    rules.put(MinorType.FIXEDCHAR, rule);

    /** FIXED16CHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.DATETIME);
    rules.put(MinorType.FIXED16CHAR, rule);

    /** FIXEDBINARY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.FIXEDBINARY, rule);

    /** VARCHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.DATETIME);
    rules.put(MinorType.VARCHAR, rule);

    /** VAR16CHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.DATETIME);
    rules.put(MinorType.VAR16CHAR, rule);

    /** VARBINARY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.DECIMAL4);
    rule.add(MinorType.DECIMAL8);
    rule.add(MinorType.DECIMAL12);
    rule.add(MinorType.DECIMAL16);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.VARBINARY, rule);
  }

  public static boolean isCastable(MajorType from, MajorType to) {
    return rules.get(to.getMinorType()) == null ? false : rules.get(
        to.getMinorType()).contains(from.getMinorType());
  }

  /*
   * code decide whether it's legal to do implicit cast. -1 : not allowed for
   * implicit cast > 0: cost associated with implicit cast. ==0: parms are
   * exactly same type of arg. No need of implicit.
   */
  public static int getCost(FunctionCall call, DrillFuncHolder holder) {
    int cost = 0;

    if (call.args.size() != holder.getParmSize()) {
      return -1;
    }
      
    for (int i = 0; i < holder.getParmSize(); i++) {
      MajorType argType = call.args.get(i).getMajorType();
      MajorType parmType = holder.getParmMajorType(i);

      if (!TypeCastRules.isCastable(argType, parmType)) {
        return -1;
      }

      Integer parmVal = ResolverTypePrecedence.precedenceMap.get(parmType
          .getMinorType());
      Integer argVal = ResolverTypePrecedence.precedenceMap.get(argType
          .getMinorType());

      if (parmVal == null) {
        throw new RuntimeException(String.format(
            "Precedence for type %s is not defined", parmType.getMinorType()
                .name()));
      }

      if (argVal == null) {
        throw new RuntimeException(String.format(
            "Precedence for type %s is not defined", argType.getMinorType()
                .name()));
      }

      if (parmVal - argVal < 0) {
        return -1;
      }
      
      // Check null vs non-null, as logic as in Types.softEqual()
      // Only when the function use NULL_IF_NULL, allow nullable <---> non-nullable.
      // Otherwise, the function implementation is not a match. 
      // We may later on use similar approach as precedenceMap. 
      if (!((holder.getNullHandling() == NullHandling.NULL_IF_NULL) &&
            (argType.getMode() == DataMode.OPTIONAL ||
             argType.getMode() == DataMode.REQUIRED ||
             parmType.getMode() == DataMode.OPTIONAL ||
             parmType.getMode() == DataMode.REQUIRED )))
          return -1;
     
      cost += (parmVal - argVal); 

      /*
      Integer parmDMVal = ResolverTypePrecedence.dmPrecedenceMap.get(parmType
          .getMode());
      Integer argDMVal = ResolverTypePrecedence.dmPrecedenceMap.get(argType
          .getMode());

      if (parmDMVal == null) {
        throw new RuntimeException(String.format(
            "Precedence for datamode %s is not defined", parmType.getMode()
                .name()));
      }

      if (argDMVal == null) {
        throw new RuntimeException(String.format(
            "Precedence for datamode %s is not defined", argType.getMode()
                .name()));
      }

      if (parmDMVal - argDMVal < 0) {
        return -1;
      }
            
      // type has higher weight than datamode
      // for arg_type of float 4 nullable, float 4 non-nullable is a better
      // match than float8 nullable.
      int base = Math.max(10, ResolverTypePrecedence.dmPrecedenceMap.size());
      cost += (parmVal - argVal) * base + (parmDMVal - argDMVal);
      */
      
    }

    return cost;
  }

}
