/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.expr;

import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ExpressionValidator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.expression.visitors.SimpleExprVisitor;
import org.apache.drill.common.types.Types;

import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.ValueReference;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;

import com.google.common.collect.Lists;

public class ImplicitCastBuilder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplicitCastBuilder.class);
   
  private ImplicitCastBuilder() {
    
  }
  
  public static LogicalExpression injectImplicitCast(LogicalExpression expr, FunctionImplementationRegistry registry, ErrorCollector errorCollector) {
    return expr.accept(new ImplicitCastVisitor(errorCollector), registry);
  }

  
  private static class ImplicitCastVisitor extends AbstractExprVisitor<LogicalExpression, FunctionImplementationRegistry, RuntimeException> {
    private final ErrorCollector errorCollector;
    private ExpressionValidator validator = new ExpressionValidator();

    public ImplicitCastVisitor(ErrorCollector errorCollector) {
          this.errorCollector = errorCollector;
    }
  
    private LogicalExpression validateNewExpr(LogicalExpression newExpr) {
      newExpr.accept(validator, errorCollector);
      return newExpr;
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, FunctionImplementationRegistry registry) throws RuntimeException {
      throw new UnsupportedOperationException(String.format("ImplicitCastVisitor does not currently support type %s.", e.getClass().getCanonicalName()));
    }

    @Override
    public LogicalExpression visitFunctionCall(FunctionCall call, FunctionImplementationRegistry registry) {
      
      List<LogicalExpression> args = Lists.newArrayList();
      for (int i = 0; i < call.args.size(); ++i) {
        LogicalExpression newExpr = call.args.get(i).accept(this, registry);
        args.add(newExpr);
      }

      //TODO : call function resolver, get the best match. Insert cast if necessary

      FunctionResolver resolver = FunctionResolverFactory.getResolver(call);    
      //create a new function call, since the argument(s) may be changed. 
      FunctionCall newCall = new FunctionCall(call.getDefinition(), args, call.getPosition()); 
      DrillFuncHolder matchedFuncHolder = resolver.getBestMatch(registry.getMethods().get(call.getDefinition().getName()), newCall); 
      
      if (matchedFuncHolder==null) {           
        return validateNewExpr(newCall);
      } else {
        List<LogicalExpression> argsWithCast = Lists.newArrayList();
        
        ValueReference[] parms = matchedFuncHolder.getParameters();
        assert parms.length == call.args.size();
        
        for (int i = 0; i < call.args.size(); ++i) {
          ValueReference param = parms[i];
          if (Types.softEquals(param.getMajorType(), call.args.get(i).getMajorType(), matchedFuncHolder.getNullHandling() == NullHandling.NULL_IF_NULL))
            argsWithCast.add(call.args.get(i));
          else {
            String castFuncName = "cast" + param.getMajorType().getMinorType().name();
            
            argsWithCast.add();
          }
        }  
        return null;
      }
    }

    @Override
    public LogicalExpression visitIfExpression(IfExpression ifExpr, FunctionImplementationRegistry registry) {
      List<IfExpression.IfCondition> conditions = Lists.newArrayList(ifExpr.conditions);
      LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, registry);

      for (int i = 0; i < conditions.size(); ++i) {
        IfExpression.IfCondition condition = conditions.get(i);

        LogicalExpression newCondition = condition.condition.accept(this, registry);
        LogicalExpression newExpr = condition.expression.accept(this, registry);
        conditions.set(i, new IfExpression.IfCondition(newCondition, newExpr));
      }

      return validateNewExpr(IfExpression.newBuilder().setElse(newElseExpr).addConditions(conditions).build());
    }

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, FunctionImplementationRegistry registry) {
      return path; 
    }

    @Override
    public LogicalExpression visitLongConstant(ValueExpressions.LongExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, FunctionImplementationRegistry registry) {
      return dExpr;
    }

    @Override
    public LogicalExpression visitBooleanConstant(ValueExpressions.BooleanExpression e, FunctionImplementationRegistry registry) {
      return e;
    }

    @Override
    public LogicalExpression visitQuotedStringConstant(ValueExpressions.QuotedString e, FunctionImplementationRegistry registry) {
      return e;
    }
  }
}
