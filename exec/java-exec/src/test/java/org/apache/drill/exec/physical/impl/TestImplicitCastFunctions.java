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
package org.apache.drill.exec.physical.impl;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.codahale.metrics.MetricRegistry;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestImplicitCastFunctions {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestImplicitCastFunctions.class);

  DrillConfig c = DrillConfig.create();
  String CAST_TEST_PHYSICAL_PLAN = "functions/cast/testICastProj.json";
  PhysicalPlanReader reader;
  FunctionImplementationRegistry registry;
  FragmentContext context;

  public Object[] getRunResult(SimpleRootExec exec) {
    int size = 0;
    for (ValueVector v : exec) {
      size++;     
    }   
  
    Object[] res = new Object [size];
    int i = 0;
    for (ValueVector v : exec) {
      res[i++] = v.getAccessor().getObject(0);
    }   
    return res;
 }
    
  public void runTest(@Injectable final DrillbitContext bitContext,
                      @Injectable UserServer.UserClientConnection connection, Object[] expectedResults) throws Throwable {

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    String planString = Resources.toString(Resources.getResource(CAST_TEST_PHYSICAL_PLAN), Charsets.UTF_8);
    if(reader == null) reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    if(registry == null) registry = new FunctionImplementationRegistry(c);
    if(context == null) context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, registry);
    PhysicalPlan plan = reader.readPhysicalPlan(planString);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    
    while(exec.next()){ 
      Object [] res = getRunResult(exec);
      assertEquals("return count does not match", res.length, expectedResults.length);
      
      for (int i = 0; i<res.length; i++) {
        assertEquals(String.format("column %s does not match", i),  res[i], expectedResults[i]);
      }
    } 

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());
  }

  @Test
  public void testInt(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable{
    // BigInt + Float8
    //runTest(bitContext, connection,"10 + 20.1", new Double(30.1));
    
    // Float8 + BigInt
    //runTest(bitContext, connection,"20.1 + 10", new Double(30.1));
    
    // Varchar + BigInt : "20" + 10
    
    Object [] expected = new Object[4];
    expected [0] = new Double (30.1);
    expected [1] = new Double (30.1);
    expected [2] = new Long (30);
    expected [3] = new Long (30);
    
    runTest(bitContext, connection, expected);
    
    /*
    runTest(bitContext, connection, "intColumn == intColumn", 100);
    runTest(bitContext, connection, "intColumn != intColumn", 0);
    runTest(bitContext, connection, "intColumn > intColumn", 0);
    runTest(bitContext, connection, "intColumn < intColumn", 0);
    runTest(bitContext, connection, "intColumn >= intColumn", 100);
    runTest(bitContext, connection, "intColumn <= intColumn", 100);
    */
  }

    @AfterClass
    public static void tearDown() throws Exception{
        // pause to get logger to catch up.
        Thread.sleep(1000);
    }
}
