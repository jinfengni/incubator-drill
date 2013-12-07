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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
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
import org.apache.drill.exec.vector.Float8Vector;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.codahale.metrics.MetricRegistry;

public class TestCastFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleFunctions.class);

  DrillConfig c = DrillConfig.create();

  @Test
  @Ignore
  public void testCastBigInt(@Injectable final DrillbitContext bitContext,
                            @Injectable UserServer.UserClientConnection connection) throws Throwable{

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/functions/cast/testCastBigInt.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    while(exec.next()){
      BigIntVector c0 = exec.getValueVectorById(new SchemaPath("output0", ExpressionPosition.UNKNOWN), BigIntVector.class);
      Float8Vector c1 = exec.getValueVectorById(new SchemaPath("output1", ExpressionPosition.UNKNOWN), Float8Vector.class);
      BigIntVector.Accessor a0;
      Float8Vector.Accessor a1;
      a0 = c0.getAccessor();
      a1 = c1.getAccessor();
      
      int count = 0;
      for(int i = 0; i < c0.getAccessor().getValueCount(); i++){        
          BigIntHolder holder0 = new BigIntHolder();
          Float8Holder holder1 = new Float8Holder();
          a0.get(i, holder0);
          a1.get(i, holder1);
          assertEquals(1, holder0.value);
          System.out.println("casted result : " + holder0.value + " original value :" + holder1.value);
          //assertEquals("aaaa", holder.toString());
          ++count;
        
      }
      assertEquals(5, count);
    }

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());

  }
    
  @After
  public void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }
}
