package org.apache.drill.exec.expr.stat;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;

import java.util.Map;

public class RangeExprEvaluator extends AbstractExprVisitor<Statistics, Void, RuntimeException> {
  private final Map<String, Statistics> columnStatMap;

  public RangeExprEvaluator(Map<String, Statistics> columnStatMap) {
    this.columnStatMap = columnStatMap;
  }

  @Override
  public Statistics visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    if (e instanceof TypedFieldExpr) {
      TypedFieldExpr fieldExpr = (TypedFieldExpr) e;
      return columnStatMap.get(fieldExpr.getName());
    }
    return null;
  }

  @Override
  public Statistics visitIntConstant(ValueExpressions.IntExpression intExpr, Void value) throws RuntimeException {
    IntStatistics intStatistics = new IntStatistics();
    intStatistics.setMinMax(intExpr.getInt(), intExpr.getInt());
    return intStatistics;
  }
}
