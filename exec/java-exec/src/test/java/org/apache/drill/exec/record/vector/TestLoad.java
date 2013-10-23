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
package org.apache.drill.exec.record.vector;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.*;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.*;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/30/13
 * Time: 5:00 PM
 */

public class TestLoad {
  @Test
  public void testLoadValueVector() {
    BufferAllocator allocator = BufferAllocator.getAllocator(null);
    ValueVector fixedV = new IntVector(
      MaterializedField.create(new SchemaPath("ints", ExpressionPosition.UNKNOWN), Types.required(MinorType.INT)),
      allocator);
    ValueVector varlenV = new VarCharVector(
      MaterializedField.create(new SchemaPath("chars", ExpressionPosition.UNKNOWN), Types.required(MinorType.VARCHAR)),
      allocator
    );
    ValueVector nullableVarlenV = new NullableVarCharVector(
      MaterializedField.create(new SchemaPath("chars", ExpressionPosition.UNKNOWN), Types.optional(MinorType.VARCHAR)),
      allocator
    );

    List<ValueVector> vectors = Lists.newArrayList(fixedV, varlenV, nullableVarlenV);
    for (ValueVector v : vectors) {
      AllocationHelper.allocate(v, 100, 50);
      v.getMutator().generateTestData();
      v.getMutator().setValueCount(100);
    }

    WritableBatch writableBatch = WritableBatch.getBatchNoHV(100, vectors, false);
    RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    ByteBuf[] byteBufs = writableBatch.getBuffers();
    int bytes = 0;
    for (int i = 0; i < byteBufs.length; i++) {
      bytes += byteBufs[i].writerIndex();
    }
    ByteBuf byteBuf = allocator.buffer(bytes);
    int index = 0;
    for (int i = 0; i < byteBufs.length; i++) {
      byteBufs[i].readBytes(byteBuf, index, byteBufs[i].writerIndex());
      index += byteBufs[i].writerIndex();
    }
    byteBuf.writerIndex(bytes);
    try {
      batchLoader.load(writableBatch.getDef(), byteBuf);
      boolean firstColumn = true;
      int recordCount = 0;
      for (VectorWrapper<?> v : batchLoader) {
        if (firstColumn) {
          firstColumn = false;
        } else {
          System.out.print("\t");
        }
        System.out.print(v.getField().getName());
        System.out.print("[");
        System.out.print(v.getField().getType().getMinorType());
        System.out.print("]");
      }

      System.out.println();
      for (int r = 0; r < batchLoader.getRecordCount(); r++) {
        boolean first = true;
        recordCount++;
        for (VectorWrapper<?> v : batchLoader) {
          if (first) {
            first = false;
          } else {
            System.out.print("\t");
          }
          ValueVector.Accessor accessor = v.getValueVector().getAccessor();
          if (v.getField().getType().getMinorType() == TypeProtos.MinorType.VARCHAR) {
            Object obj = accessor.getObject(r) ;
            if(obj != null)
              System.out.print(new String((byte[]) accessor.getObject(r)));
            else
              System.out.print("NULL");
          } else {
            System.out.print(accessor.getObject(r));
          }
        }
        if (!first) System.out.println();
      }
      assertEquals(100, recordCount);
    } catch (Exception e) {
        e.printStackTrace();
    }
  }

}
