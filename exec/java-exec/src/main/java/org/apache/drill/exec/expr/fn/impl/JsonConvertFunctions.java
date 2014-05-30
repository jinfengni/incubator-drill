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

import java.io.ByteArrayOutputStream;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.base.Charsets;

public class JsonConvertFunctions {

 static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctions.class);

  private JsonConvertFunctions(){}

  /*
   * convert mock functions.
   */

  @FunctionTemplate(name = "convert_toJSON", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ConvertToJson implements DrillSimpleFunc{

    @Param FieldReader input;
    @Output VarBinaryHolder out;
    @Workspace ByteBuf buffer;

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }

    public void eval(){
      //String typeStr = input.getType().toString();

      out.buffer = buffer;
      out.start = 0;

      java.io.ByteArrayOutputStream stream = new java.io.ByteArrayOutputStream();
      try {
        org.apache.drill.exec.vector.complex.fn.JsonWriter jsonWriter = new org.apache.drill.exec.vector.complex.fn.JsonWriter(stream, true);

        jsonWriter.write(input);
      } catch (Exception e) {
        System.out.println(" msg = " + e.getMessage() + " trace : " + e.getStackTrace());
      }

      byte [] bytea = stream.toByteArray();

      out.buffer.setBytes(out.start, bytea);
      out.end = bytea.length;

//      System.out.println("byte = " + new String(bytea));
//      System.out.println("Read type = " + input.getType().toString());

    }
  }
}
