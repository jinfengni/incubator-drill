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
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

public class CastFunctionsVarLen {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CastFunctionsVarLen.class);
  
  private CastFunctionsVarLen(){}
  
  @FunctionTemplate(name = "castBigInt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarBinaryToBigInt implements DrillSimpleFunc{

    @Param VarBinaryHolder in;
    @Output BigIntHolder out;

    public void setup(RecordBatch b) {}

    public void eval() {
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Long.parseLong(new String(buf));
    }
  }

  @FunctionTemplate(name = "castBigInt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharToBigInt implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output BigIntHolder out;

    public void setup(RecordBatch b) {}

    public void eval() {
       byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Long.parseLong(new String(buf));
    }
  }

  @FunctionTemplate(name = "castVarchar", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastIntToVarchar implements DrillSimpleFunc{

    @Param IntHolder in;
    @Workspace ByteBuf buffer;     
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
    //max value of java long is 2^32, which has 10 digits.
      buffer = incoming.getContext().getAllocator().buffer(10);      
    }

    public void eval() {      
      String istr = (new Integer(in.value)).toString();
      out.buffer = buffer;
      out.start = 0;
      out.end = istr.length();
      out.buffer.setBytes(0, istr.getBytes());      
    }
  }
  
  @FunctionTemplate(name = "castVarchar", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastBigIntToVarchar implements DrillSimpleFunc{

    @Param BigIntHolder in;
    @Param BigIntHolder len;
    @Workspace ByteBuf buffer; 
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
      //max value of java long is 2^64, which has 20 digits.
      buffer = incoming.getContext().getAllocator().buffer(20);
    }

    public void eval() {
      String lstr = (new Long(in.value)).toString();
      out.buffer = buffer;
      out.start = 0;
      out.end = lstr.length();
      out.buffer.setBytes(0, lstr.getBytes());      
    }
  }

}
