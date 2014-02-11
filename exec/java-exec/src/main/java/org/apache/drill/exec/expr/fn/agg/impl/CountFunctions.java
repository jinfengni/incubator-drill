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
package org.apache.drill.exec.expr.fn.agg.impl;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class CountFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CountFunctions.class);
  
  @FunctionTemplate(name = "count", scope = FunctionScope.POINT_AGGREGATE)
  public static class BigIntCount implements DrillAggFunc{

    @Param BigIntHolder in;
    @Workspace BigIntHolder count;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
      count = new BigIntHolder();
      count.value = 0;
    }

    @Override
    public void add() {
      count.value++;
    }

    @Override
    public void output() {
      out.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
    
  }
  
  @FunctionTemplate(name = "count", scope = FunctionScope.POINT_AGGREGATE)
  public static class IntCount implements DrillAggFunc{

    @Param IntHolder in;
    @Workspace IntHolder count;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
      count = new IntHolder();
      count.value = 0;
    }

    @Override
    public void add() {
      count.value++;
    }

    @Override
    public void output() {
      out.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
    
  }
}
