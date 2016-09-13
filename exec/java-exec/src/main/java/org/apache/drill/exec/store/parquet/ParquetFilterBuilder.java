/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.stat.ParquetPredicates;
import org.apache.drill.exec.expr.stat.TypedFieldExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ParquetFilterBuilder extends AbstractExprVisitor<LogicalExpression, Void, RuntimeException> {
  static final Logger logger = LoggerFactory.getLogger(ParquetFilterBuilder.class);

  static final ParquetFilterBuilder FILTER_BUILDER = new ParquetFilterBuilder();

  public static LogicalExpression buildParquetFilterPredicate(LogicalExpression expr) {
    final LogicalExpression predicate = expr.accept(FILTER_BUILDER, null);
    return predicate;
  }

  private ParquetFilterBuilder() {
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, Void value) {
    if (e instanceof TypedFieldExpr) {
      return e;
    }

    return null;
  }

  @Override
  public LogicalExpression visitIntConstant(ValueExpressions.IntExpression intExpr, Void value)
      throws RuntimeException {
    return intExpr;
  }

  @Override
  public LogicalExpression visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Void value)
      throws RuntimeException {
    return dExpr;
  }

  @Override
  public LogicalExpression visitFloatConstant(ValueExpressions.FloatExpression fExpr, Void value)
      throws RuntimeException {
    return fExpr;
  }

  @Override
  public LogicalExpression visitLongConstant(ValueExpressions.LongExpression intExpr, Void value)
      throws RuntimeException {
    return intExpr;
  }

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator op, Void value) {
    List<LogicalExpression> childPredicates = new ArrayList<>();
    String functionName = op.getName();

    for (LogicalExpression arg : op.args) {
      LogicalExpression childPredicate = arg.accept(this, null);
      if (childPredicate == null) {
        if (functionName.equals("booleanOr")) {
          // we can't include any leg of the OR if any of the predicates cannot be converted
          return null;
        }
      } else {
        childPredicates.add(childPredicate);
      }
    }

    if (childPredicates.size() == 0) {
      return null; // none leg is qualified, return null.
    } else if (childPredicates.size() == 1) {
      return childPredicates.get(0); // only one leg is qualified, remove boolean op.
    } else {
      if (functionName.equals("booleanOr")) {
        return new ParquetPredicates.OrPredicate(op.getName(), childPredicates, op.getPosition());
      } else {
        return new ParquetPredicates.AndPredicate(op.getName(), childPredicates, op.getPosition());
      }
    }
  }

  @Override
  public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression funcHolderExpr, Void value)
      throws RuntimeException {
    FuncHolder holder = funcHolderExpr.getHolder();

    if (! (holder instanceof DrillSimpleFuncHolder)) {
      return null;
    }

    if (isCompareFunction(((DrillSimpleFuncHolder) holder).getRegisteredNames()[0])) {
      return handleCompareFunction(funcHolderExpr, value);
    }

    return null;
  }

  private LogicalExpression handleCompareFunction(FunctionHolderExpression functionHolderExpression, Void value) {
    for (LogicalExpression arg : functionHolderExpression.args) {
      LogicalExpression newArg = arg.accept(this, value);
      if (newArg == null) {
        return null;
      }
    }

    String funcName = ((DrillSimpleFuncHolder) functionHolderExpression.getHolder()).getRegisteredNames()[0];

    switch (funcName) {
    case FunctionGenerationHelper.EQ :
      return new ParquetPredicates.EqualPredicate(functionHolderExpression.args.get(0), functionHolderExpression.args.get(1));
    case FunctionGenerationHelper.GT :
      return new ParquetPredicates.GTPredicate(functionHolderExpression.args.get(0), functionHolderExpression.args.get(1));
    case FunctionGenerationHelper.GE :
      return new ParquetPredicates.GEPredicate(functionHolderExpression.args.get(0), functionHolderExpression.args.get(1));
    case FunctionGenerationHelper.LT :
      return new ParquetPredicates.LTPredicate(functionHolderExpression.args.get(0), functionHolderExpression.args.get(1));
    case FunctionGenerationHelper.LE :
      return new ParquetPredicates.LEPredicate(functionHolderExpression.args.get(0), functionHolderExpression.args.get(1));
    case FunctionGenerationHelper.NE :
      return new ParquetPredicates.NEPredicate(functionHolderExpression.args.get(0), functionHolderExpression.args.get(1));
    default:
      return null;
    }
  }

  private static boolean isCompareFunction(String funcName) {
    return COMPARE_FUNCTIONS_SET.contains(funcName);
  }

  private static final ImmutableSet<String> COMPARE_FUNCTIONS_SET;

  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    COMPARE_FUNCTIONS_SET = builder
        .add(FunctionGenerationHelper.EQ)
        .add(FunctionGenerationHelper.GT)
        .add(FunctionGenerationHelper.GE)
        .add(FunctionGenerationHelper.LT)
        .add(FunctionGenerationHelper.LE)
        .add(FunctionGenerationHelper.NE)
        .build();
  }

}
