package org.apache.drill.exec.planner.physical;

import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.fn.MathFunctions;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.expr.fn.impl.HashFunctions;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.reltype.RelDataType;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;

public class PrelUtil {

  public static List<Ordering> getOrdering(RelCollation collation, RelDataType rowType) {
    List<Ordering> orderExpr = Lists.newArrayList();
    
    final List<String> childFields = rowType.getFieldNames();
    
    for (RelFieldCollation fc: collation.getFieldCollations() ) {      
      FieldReference fr = new FieldReference(childFields.get(fc.getFieldIndex()), ExpressionPosition.UNKNOWN);
      orderExpr.add(new Ordering(fc.getDirection(), fr, fc.nullDirection));
    }
    
    return orderExpr;
  }

  /*
   * Return a hash expression :  hash(field1) ^ hash(field2) ^ hash(field3) ... ^ hash(field_n)
   */
  public static LogicalExpression getHashExpression(List<DistributionField> fields, RelDataType rowType) {
    assert fields.size() > 0;
    
    final List<String> childFields = rowType.getFieldNames();
    
    FieldReference fr = new FieldReference(childFields.get(fields.get(0).getFieldId()), ExpressionPosition.UNKNOWN);    
    FunctionCall func = new FunctionCall(HashFunctions.HASH,  ImmutableList.of((LogicalExpression)fr), ExpressionPosition.UNKNOWN);
    
    for (int i = 1; i<fields.size(); i++) {     
      fr = new FieldReference(childFields.get(fields.get(i).getFieldId()), ExpressionPosition.UNKNOWN);      
      FunctionCall func2 = new FunctionCall(HashFunctions.HASH,  ImmutableList.of((LogicalExpression)fr), ExpressionPosition.UNKNOWN);
      
      func = new FunctionCall(MathFunctions.XOR, ImmutableList.of((LogicalExpression)func, (LogicalExpression)func2), ExpressionPosition.UNKNOWN);
    }
    
    return func;
  }

}
