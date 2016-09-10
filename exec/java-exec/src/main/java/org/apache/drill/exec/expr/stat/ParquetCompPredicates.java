package org.apache.drill.exec.expr.stat;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.LogicalExpressionBase;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.parquet.column.statistics.Statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract  class ParquetCompPredicates {
  public static abstract  class ParquetCompPredicate extends LogicalExpressionBase {
    protected final LogicalExpression left;
    protected final LogicalExpression right;

    public ParquetCompPredicate(LogicalExpression left, LogicalExpression right) {
      super(left.getPosition());
      this.left = left;
      this.right = right;
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      final List<LogicalExpression> args = new ArrayList<>();
      args.add(left);
      args.add(right);
      return args.iterator();
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitUnknown(this, value);
    }

    public abstract boolean canDrop(RangeExprEvaluator evaluator);
  }

  public static class EqualPredicate extends ParquetCompPredicate {
    public EqualPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      if ( ( leftStat.genericGetMax().compareTo(rightStat.genericGetMin()) < 0
            || rightStat.genericGetMax().compareTo(leftStat.genericGetMin()) < 0)) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return left.toString()  + " = " + right.toString();
    }
  }

}
