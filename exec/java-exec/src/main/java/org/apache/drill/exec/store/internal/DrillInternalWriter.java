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
package org.apache.drill.exec.store.internal;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.cache.VectorContainerSerializable;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.DrillInternalWriterConfig;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.IntVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DrillInternalWriter extends AbstractSingleRecordBatch<DrillInternalWriterConfig> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillInternalWriter.class);

  private final FSDataOutputStream output;
  private final SchemaPath schemaPath = new SchemaPath("recordsWritten", ExpressionPosition.UNKNOWN);

  public DrillInternalWriter(DrillInternalWriterConfig popConfig, RecordBatch incoming, FragmentContext context) {
    super(popConfig, context, incoming);
    try {
      if (!popConfig.getNullOutput()) {
        Configuration conf = new Configuration();
        conf.set("fs.name.default", popConfig.getDfsName());
          FileSystem fs = FileSystem.get(conf);
          Path outputDir = new Path(popConfig.getPath());
          if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
          }
          Path path = new Path(outputDir, String.format("part_%05d", context.getHandle().getMinorFragmentId()));
          output = fs.create(path);
      } else {
        output = new FSDataOutputStream(ByteStreams.nullOutputStream());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public int getRecordCount() {
    return 1;
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    TypeProtos.MajorType type = TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.INT).setMode(TypeProtos.DataMode.REQUIRED).build();
    MaterializedField outputField = MaterializedField.create(schemaPath, type);
    IntVector out = new IntVector(outputField, context.getAllocator());
    out.allocateNew(1);
    container.clear();
    container.add(out);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  @Override
  protected void doWork() {
    VectorContainerSerializable wrap = new VectorContainerSerializable(incoming);
    try {
      Stopwatch watch = new Stopwatch();
      watch.start();
      wrap.writeToStream(output);
      logger.debug("Wrote {} records in {} us", incoming.getRecordCount(), watch.elapsed(TimeUnit.MICROSECONDS));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    IntVector vector = (IntVector)container.getValueAccessorById(container.getValueVectorId(schemaPath).getFieldId(), IntVector.class).getValueVector();
    vector.allocateNew(1);
    vector.getMutator().set(0, incoming.getRecordCount());
    vector.getMutator().setValueCount(1);
  }
}
