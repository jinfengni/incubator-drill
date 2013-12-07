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
package org.apache.drill.common.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.util.PathScanner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class FunctionRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionRegistry.class);
  
  private final Map<String, FunctionDefinition> funcMap;

  private static final Map<String, String> typeNameMap;
  static {
    typeNameMap = new HashMap<String, String> ();
    typeNameMap.put("int", "Int");
    typeNameMap.put("bigint", "BigInt");
    typeNameMap.put("float4", "Float4");
    typeNameMap.put("float8", "Float8");
    typeNameMap.put("varchar", "VarChar");
    typeNameMap.put("varbinary", "VarBinary");
  }
  public FunctionRegistry(DrillConfig config){
    try{
      Set<Class<? extends CallProvider>> providerClasses = PathScanner.scanForImplementations(CallProvider.class, config.getStringList(CommonConstants.LOGICAL_FUNCTION_SCAN_PACKAGES));
      Map<String, FunctionDefinition> funcs = new HashMap<String, FunctionDefinition>();
      for (Class<? extends CallProvider> c : providerClasses) {
        CallProvider p = c.newInstance();
        FunctionDefinition[] defs = p.getFunctionDefintions();
        for(FunctionDefinition d : defs){
          for(String rn : d.getRegisteredNames()){
            
            FunctionDefinition d2 = funcs.put(rn, d);
//            logger.debug("Registering function {}", d);
            if(d2 != null){
              throw new ExceptionInInitializerError(String.format("Failure while registering functions.  The function %s tried to register with the name %s but the function %s already registered with that name.", d.getName(), rn, d2.getName()) );
            }
          }
        }
      }
      funcMap = funcs;
    }catch(Exception e){
      throw new RuntimeException("Failure while setting up FunctionRegistry.", e);
    }
  }
  
  /*
   * create a cast function specific to a target type with fixed size. 
   * The original input args :  LogicalExpression  input_expr, String target_type, Boolean repeat ?
   * The new cast function's name : cast+target_type.
   * The new cast function's args : input_expr. 
   */
  public LogicalExpression createCastFixedSize(String functionName, ExpressionPosition ep, List<LogicalExpression> args){
    Preconditions.checkArgument(args.size() >= 2 && args.get(1) != null && args.get(1) instanceof QuotedString, "Wrong arguments of cast functions.");
    
    String targetType = typeNameMap.get(((QuotedString) args.get(1)).value);
    String castFuncWithType = functionName + targetType;
    
    FunctionDefinition d = funcMap.get(castFuncWithType);
    if(d == null) throw new ExpressionParsingException(String.format("Unable to find function definition for function named '%s'", castFuncWithType));
    
    List<LogicalExpression> newArgs = Lists.newArrayList();
    newArgs.add(args.get(0));  //input_expr
    /*
    OutputTypeDeterminer otd;
    
    switch(targetType) {
    case "BigInt" : otd = OutputTypeDeterminer.FIXED_BIGINT; break;
    case "Int"    : otd = OutputTypeDeterminer.FIXED_INT; break;
    case "Float4"    : otd = OutputTypeDeterminer.FIXED_FLOAT4; break;
    case "Float8"    : otd = OutputTypeDeterminer.FIXED_FLOAT8; break;
    default: otd = OutputTypeDeterminer.FIXED_BIGINT;
    }
    */
    FunctionDefinition castFuncDef = FunctionDefinition.simple(castFuncWithType, d.getArgumentValidator(), d.getOutputTypeDeterminer()) ;   
    
    return new FunctionCall(castFuncDef, newArgs, ep);
  }
  
  /*
   * create a cast function specific to a target type with var size. 
   * The original input args :  LogicalExpression  input_expr, String target_type, Numeric target_type_length, Boolean repeat ?
   * The new cast function's name : cast+target_type.
   * The new cast function's args : input_expr, target_type_length 
   */
  public LogicalExpression createCastVarSize(String functionName, ExpressionPosition ep, List<LogicalExpression> args){
    Preconditions.checkArgument(args.size() >= 3 && args.get(1) != null && args.get(1) instanceof QuotedString, "Wrong arguments of cast functions.");
    
    String targetType = typeNameMap.get(((QuotedString) args.get(1)).value);     
    String castFuncWithType = functionName + targetType;
    
    FunctionDefinition d = funcMap.get(castFuncWithType);
    if(d == null) throw new ExpressionParsingException(String.format("Unable to find function definition for function named '%s'", castFuncWithType));
    
    List<LogicalExpression> newArgs = Lists.newArrayList();
    newArgs.add(args.get(0));  //input_expr
    newArgs.add(args.get(2));  //type_length
    /*
    OutputTypeDeterminer otd;
    switch(targetType) {
    case "VarChar" : otd = OutputTypeDeterminer.FIXED_VARCHAR; break;
    case "Var16Char" : otd = OutputTypeDeterminer.FIXED_VAR16CHAR; break;
    case "VarBinary" : otd = OutputTypeDeterminer.FIXED_VARBINARY; break;
    default: otd = OutputTypeDeterminer.FIXED_VARCHAR;
    }
    */
    
    FunctionDefinition castFuncDef = FunctionDefinition.simple(castFuncWithType, d.getArgumentValidator(), d.getOutputTypeDeterminer()) ;   
    
    return new FunctionCall(castFuncDef, newArgs, ep);
  }
  
  public LogicalExpression createExpression(String functionName, ExpressionPosition ep, List<LogicalExpression> args){
    FunctionDefinition d = funcMap.get(functionName);
    if(d == null) throw new ExpressionParsingException(String.format("Unable to find function definition for function named '%s'", functionName));
    return d.newCall(args, ep);
  }
  
  public LogicalExpression createExpression(String unaryName, ExpressionPosition ep, LogicalExpression... e){
    return funcMap.get(unaryName).newCall(Lists.newArrayList(e), ep);
  }
  
  public LogicalExpression createByOp(List<LogicalExpression> args, ExpressionPosition ep, List<String> opTypes) {
    // logger.debug("Generating new comparison expressions.");
    if (args.size() == 1) {
      return args.get(0);
    }

    if (args.size() - 1 != opTypes.size())
      throw new DrillRuntimeException("Must receive one more expression then the provided number of operators.");

    LogicalExpression first = args.get(0);
    for (int i = 0; i < opTypes.size(); i++) {
      List<LogicalExpression> l2 = new ArrayList<LogicalExpression>();
      l2.add(first);
      l2.add(args.get(i + 1));
      first = createExpression(opTypes.get(i), ep, args);
    }
    return first;
  }
}
