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
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

public class CastFunctionsVarLen {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CastFunctionsVarLen.class);
  
  private CastFunctionsVarLen(){}
  
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // target_type : BigInt
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
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

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // target_type : Int
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
  @FunctionTemplate(name = "castInt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarBinaryToInt implements DrillSimpleFunc{

    @Param VarBinaryHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch b) {}

    public void eval() {
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Integer.parseInt(new String(buf));
    }
  }

  @FunctionTemplate(name = "castInt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharToInt implements DrillSimpleFunc{
    @Param VarCharHolder in;
    @Output IntHolder out;

    public void setup(RecordBatch b) {}

    public void eval() {
       byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Integer.parseInt(new String(buf));
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // target_type : Float4
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
  @FunctionTemplate(name = "castFloat4", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarBinaryToFloat4 implements DrillSimpleFunc{

    @Param VarBinaryHolder in;
    @Output Float4Holder out;

    public void setup(RecordBatch b) {}

    public void eval() {
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Float.parseFloat(new String(buf));
    }
  }

  @FunctionTemplate(name = "castFloat4", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharToFloat4 implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output Float4Holder out;

    public void setup(RecordBatch b) {}

    public void eval() {
       byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Float.parseFloat(new String(buf));
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // target_type : Float8
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
  @FunctionTemplate(name = "castFloat8", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarBinaryToFloat8 implements DrillSimpleFunc{

    @Param VarBinaryHolder in;
    @Output Float8Holder out;

    public void setup(RecordBatch b) {}

    public void eval() {
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Double.parseDouble(new String(buf));
    }
  }

  @FunctionTemplate(name = "castFloat8", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharToFloat8 implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output Float8Holder out;

    public void setup(RecordBatch b) {}

    public void eval() {
       byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      out.value = Double.parseDouble(new String(buf));
    }
  }
  
  
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // target_type : Varchar
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  @FunctionTemplate(name = "castVarchar", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastIntToVarchar implements DrillSimpleFunc{

    @Param IntHolder in;
    @Param BigIntHolder len;
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
      out.end = Math.min((int)len.value, istr.length()); // truncate if target varchar has length smaller than # of input's digits      
      out.buffer.setBytes(0, istr.substring(0,out.end).getBytes());      
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
      out.end = Math.min((int)len.value, lstr.length()); // truncate if target varchar has length smaller than # of input's digits 
      out.buffer.setBytes(0, lstr.substring(0, out.end).getBytes());      
    }
  }

  @FunctionTemplate(name = "castVarchar", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastFloat4ToVarchar implements DrillSimpleFunc{

    @Param Float4Holder in;
    @Param BigIntHolder len;
    @Workspace ByteBuf buffer;     
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
      //TODO: assume float4 has 100 digits
      buffer = incoming.getContext().getAllocator().buffer(100);      
    }

    public void eval() {      
      String fstr = (new Float(in.value)).toString();
      out.buffer = buffer;
      out.start = 0;
      out.end = Math.min((int)len.value, fstr.length()); // truncate if target varchar has length smaller than # of input's digits      
      out.buffer.setBytes(0, fstr.substring(0,out.end).getBytes());      
    }
  }
  
  @FunctionTemplate(name = "castVarchar", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastFloat8ToVarchar implements DrillSimpleFunc{

    @Param Float8Holder in;
    @Param BigIntHolder len;
    @Workspace ByteBuf buffer;     
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
      //TODO: assume float4 has 100 digits
      buffer = incoming.getContext().getAllocator().buffer(100);      
    }

    public void eval() {      
      String fstr = (new Double(in.value)).toString();
      out.buffer = buffer;
      out.start = 0;
      out.end = Math.min((int)len.value, fstr.length()); // truncate if target varchar has length smaller than # of input's digits      
      out.buffer.setBytes(0, fstr.substring(0,out.end).getBytes());      
    }
  }
  
}
