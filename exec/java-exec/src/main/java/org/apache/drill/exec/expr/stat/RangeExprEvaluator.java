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

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.joda.time.DateTimeUtils;

import java.util.Map;

import static org.apache.drill.exec.store.ParquetOutputRecordWriter.JULIAN_DAY_EPOC;

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
  public Statistics visitIntConstant(ValueExpressions.IntExpression expr, Void value) throws RuntimeException {
    final IntStatistics intStatistics = new IntStatistics();
    intStatistics.setMinMax(expr.getInt(), expr.getInt());
    return intStatistics;
  }

  @Override
  public Statistics visitLongConstant(ValueExpressions.LongExpression expr, Void value) throws RuntimeException {
    final LongStatistics longStatistics = new LongStatistics();
    longStatistics.setMinMax(expr.getLong(), expr.getLong());
    return longStatistics;
  }

  @Override
  public Statistics visitFloatConstant(ValueExpressions.FloatExpression expr, Void value) throws RuntimeException {
    final FloatStatistics floatStatistics = new FloatStatistics();
    floatStatistics.setMinMax(expr.getFloat(), expr.getFloat());
    return floatStatistics;
  }

  @Override
  public Statistics visitDoubleConstant(ValueExpressions.DoubleExpression expr, Void value) throws RuntimeException {
    final DoubleStatistics doubleStatistics = new DoubleStatistics();
    doubleStatistics.setMinMax(expr.getDouble(), expr.getDouble());
    return doubleStatistics;
  }

  @Override
  public Statistics visitDateConstant(ValueExpressions.DateExpression expr, Void value) throws RuntimeException {
    final IntStatistics intStatistics = new IntStatistics();
    long dateInMillis = expr.getDate();

    // Specific for date column created by Drill CTAS prior fix for DRILL-4203.
    // Apply the same shit as what in ParquetOutputRecordWriter.java for data value.
    int intValue = (int) (DateTimeUtils.toJulianDayNumber(dateInMillis) + JULIAN_DAY_EPOC);
    intStatistics.setMinMax(intValue, intValue);
    return intStatistics;
  }
}
