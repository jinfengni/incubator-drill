/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.stat;

import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.joda.time.DateTimeUtils;
import sun.security.util.BigInt;

import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.ParquetOutputRecordWriter.JULIAN_DAY_EPOC;

public class RangeExprEvaluator extends AbstractExprVisitor<Statistics, Void, RuntimeException> {
  private final Map<String, Statistics> columnStatMap;
  private final Set<LogicalExpression> constantBoundaries;
  private final UdfUtilities udfUtilities;

  public RangeExprEvaluator(final Map<String, Statistics> columnStatMap, final Set<LogicalExpression> constantBoundaries, UdfUtilities udfUtilities) {
    this.columnStatMap = columnStatMap;
    this.constantBoundaries = constantBoundaries;
    this.udfUtilities = udfUtilities;
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
  public Statistics visitIntConstant(ValueExpressions.IntExpression expr, Void value) throws RuntimeException {
//    final IntStatistics intStatistics = new IntStatistics();
//    intStatistics.setMinMax(expr.getInt(), expr.getInt());
//    return intStatistics;
    return getStatistics(expr.getInt());
  }

  @Override
  public Statistics visitLongConstant(ValueExpressions.LongExpression expr, Void value) throws RuntimeException {
//    final LongStatistics longStatistics = new LongStatistics();
//    longStatistics.setMinMax(expr.getLong(), expr.getLong());
//    return longStatistics;
    return getStatistics(expr.getLong());
  }

  @Override
  public Statistics visitFloatConstant(ValueExpressions.FloatExpression expr, Void value) throws RuntimeException {
//    final FloatStatistics floatStatistics = new FloatStatistics();
//    floatStatistics.setMinMax(expr.getFloat(), expr.getFloat());
//    return floatStatistics;
    return getStatistics(expr.getFloat());
  }

  @Override
  public Statistics visitDoubleConstant(ValueExpressions.DoubleExpression expr, Void value) throws RuntimeException {
//    final DoubleStatistics doubleStatistics = new DoubleStatistics();
//    doubleStatistics.setMinMax(expr.getDouble(), expr.getDouble());
//    return doubleStatistics;
    return getStatistics(expr.getDouble());
  }

  @Override
  public Statistics visitDateConstant(ValueExpressions.DateExpression expr, Void value) throws RuntimeException {
    final IntStatistics intStatistics = new IntStatistics();
    long dateInMillis = expr.getDate();

    int intValue = convertDrillDateValue(dateInMillis);
    return getStatistics(intValue);
  }

  @Override
  public Statistics visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
    if (constantBoundaries.contains(holder)) {
      ValueHolder result = InterpreterEvaluator.evaluateConstantExpr(udfUtilities, holder);

      switch (holder.getMajorType().getMinorType()) {
      case INT :
        return getStatistics(((IntHolder) result).value);
      case DATE:
        int intValue = convertDrillDateValue(((DateHolder) result).value);
        return getStatistics(intValue);
      case BIGINT:
        return getStatistics(((BigIntHolder) result).value);
      case FLOAT4:
        return getStatistics(((Float4Holder)result).value);
      case FLOAT8:
        return getStatistics(((Float8Holder)result).value);
      default:
        // defautl return null to indicate filter containing that constant expression is not good for filter pushdown.
        return null;
      }
    } else {
      FuncHolder funcHold = holder.getHolder();

      if (! (funcHold instanceof DrillSimpleFuncHolder)) {
        // Only Drill function is allowed.
        return null;
      }
      final String funcName = ((DrillSimpleFuncHolder) funcHold).getRegisteredNames()[0];

    }
    return null;
  }

  private IntStatistics getStatistics(int value) {
    final IntStatistics intStatistics = new IntStatistics();
    intStatistics.setMinMax(value, value);
    return intStatistics;
  }

  private LongStatistics getStatistics(long value) {
    final LongStatistics longStatistics = new LongStatistics();
    longStatistics.setMinMax(value, value);
    return longStatistics;
  }

  private DoubleStatistics getStatistics(double value) {
    final DoubleStatistics doubleStatistics = new DoubleStatistics();
    doubleStatistics.setMinMax(value, value);
    return doubleStatistics;
  }

  private FloatStatistics getStatistics(float value) {
    final FloatStatistics floatStatistics = new FloatStatistics();
    floatStatistics.setMinMax(value, value);
    return floatStatistics;
  }

  private int convertDrillDateValue(long dateInMillis) {
    // Specific for date column created by Drill CTAS prior fix for DRILL-4203.
    // Apply the same shit as what in ParquetOutputRecordWriter.java for data value.
    final int intValue = (int) (DateTimeUtils.toJulianDayNumber(dateInMillis) + JULIAN_DAY_EPOC);
    return intValue;
  }

}
