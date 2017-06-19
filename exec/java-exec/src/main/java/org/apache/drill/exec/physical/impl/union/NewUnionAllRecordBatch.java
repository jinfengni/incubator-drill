/*
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
package org.apache.drill.exec.physical.impl.union;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import static org.apache.drill.exec.physical.impl.union.UnionAllRecordBatch.hasSameTypeAndMode;

public class NewUnionAllRecordBatch extends AbstractRecordBatch<UnionAll> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NewUnionAllRecordBatch.class);

  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  private UnionAller unionall;
  private final RecordBatch left;
  private final RecordBatch right;
  private final List<TransferPair> transfers = Lists.newArrayList();
  private List<ValueVector> allocationVectors = Lists.newArrayList();
  private int recordCount = 0;
  private Stack<BatchStatusWrappper> batchStatusStack = new Stack<>();


  public NewUnionAllRecordBatch(UnionAll config, List<RecordBatch> children, FragmentContext context) throws OutOfMemoryException {
    super(config, context, true);
    Preconditions.checkArgument(children.size() == 2, "The number of the operands of Union must be 2");
    left = children.get(0);
    right = children.get(1);
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    left.kill(sendUpstream);
    right.kill(sendUpstream);
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    IterOutcome leftUpstream = next(0, left);
    IterOutcome rightUpstream = next(1, right);

    if (leftUpstream == IterOutcome.STOP || rightUpstream == IterOutcome.STOP) {
      state = BatchState.STOP;
      return;
    }

    if (leftUpstream == IterOutcome.OUT_OF_MEMORY || rightUpstream == IterOutcome.OUT_OF_MEMORY) {
      state = BatchState.OUT_OF_MEMORY;
      return;
    }

    if (leftUpstream == IterOutcome.NONE && rightUpstream == IterOutcome.NONE) {
      state = BatchState.DONE;
      return;
    }

    if (rightUpstream == IterOutcome.OK_NEW_SCHEMA) {
      batchStatusStack.push(new BatchStatusWrappper(true, right, 1));
    }
    if (leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
      batchStatusStack.push(new BatchStatusWrappper(true, left, 0));
    }

    if (leftUpstream == IterOutcome.NONE && rightUpstream == IterOutcome.OK_NEW_SCHEMA) {
      inferOutputFieldsOneSide(right.getSchema());
    } else if (rightUpstream == IterOutcome.NONE && leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
      inferOutputFieldsOneSide((left.getSchema()));
    } else if (leftUpstream == IterOutcome.OK_NEW_SCHEMA && rightUpstream == IterOutcome.OK_NEW_SCHEMA) {
      inferOutputFieldsBothSide(left.getSchema(), right.getSchema());
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    for (VectorWrapper vv: container) {
      vv.getValueVector().allocateNew();
      vv.getValueVector().getMutator().setValueCount(0);
    }
  }

  @Override
  public IterOutcome innerNext() {
    try {
      Pair<IterOutcome, RecordBatch> nextBatch = getNextBatch();

      logger.debug("Upstream of Union-All: {}", nextBatch.left);

      switch(nextBatch.left) {
        case NONE:
        case OUT_OF_MEMORY:
        case STOP:
          return nextBatch.left;
        case OK_NEW_SCHEMA:
          return doWork(nextBatch.right, true);
        case OK:
          return doWork(nextBatch.right, false);
        default:
          throw new IllegalStateException(String.format("Unknown state %s.", nextBatch.left));
      }
    } catch (ClassTransformationException | IOException | SchemaChangeException ex) {
      context.fail(ex);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }


  @SuppressWarnings("resource")
  private IterOutcome doWork(RecordBatch inputBatch, boolean newSchema) throws ClassTransformationException, IOException, SchemaChangeException {
    if (inputBatch.getSchema().getFieldCount() != container.getSchema().getFieldCount()) {
      // wrong.
    }

    if (newSchema) {
      createUnionAller(inputBatch);
    }

    container.zeroVectors();

    for (final ValueVector v : this.allocationVectors) {
      AllocationHelper.allocateNew(v, inputBatch.getRecordCount());
    }

    recordCount = unionall.unionRecords(0, inputBatch.getRecordCount(), 0);
    for (final ValueVector v : allocationVectors) {
      final ValueVector.Mutator m = v.getMutator();
      m.setValueCount(recordCount);
    }

    if (callBack.getSchemaChangedAndReset()) {
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
    }
  }

  private void createUnionAller(RecordBatch inputBatch) throws ClassTransformationException, IOException, SchemaChangeException {
    transfers.clear();
    allocationVectors.clear();;

    final ClassGenerator<UnionAller> cg = CodeGenerator.getRoot(UnionAller.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    cg.getCodeGenerator().plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    //    cg.getCodeGenerator().saveCodeForDebugging(true);

    int index = 0;
    for(VectorWrapper<?> vw : inputBatch) {
      ValueVector vvIn = vw.getValueVector();
      ValueVector vvOut = container.getValueVector(index).getValueVector();

      final ErrorCollector collector = new ErrorCollectorImpl();
      // According to input data names, Minortypes, Datamodes, choose to
      // transfer directly,
      // rename columns or
      // cast data types (Minortype or DataMode)
      if (hasSameTypeAndMode(container.getSchema().getColumn(index), vvIn.getField()) &&
          vvIn.getField().getType().getMinorType() != TypeProtos.MinorType.MAP // Per DRILL-5521, existing bug for map transfer
          ) {
        // Transfer column
        TransferPair tp = vvIn.makeTransferPair(vvOut);
        transfers.add(tp);
        // Copy data in order to rename the column
      } else {
        SchemaPath inputPath = SchemaPath.getSimplePath(vvIn.getField().getPath());
        MaterializedField inField = vvIn.getField();
        MaterializedField outputField = vvOut.getField();

        LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath, inputBatch, collector, context.getFunctionRegistry());

        if (collector.hasErrors()) {
          throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
        }

        // If the inputs' DataMode is required and the outputs' DataMode is not required
        // cast to the one with the least restriction
        if(inField.getType().getMode() == TypeProtos.DataMode.REQUIRED
            && outputField.getType().getMode() != TypeProtos.DataMode.REQUIRED) {
          expr = ExpressionTreeMaterializer.convertToNullableType(expr, inField.getType().getMinorType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        // If two inputs' MinorTypes are different,
        // Insert a cast before the Union operation
        if(inField.getType().getMinorType() != outputField.getType().getMinorType()) {
          expr = ExpressionTreeMaterializer.addCastExpression(expr, outputField.getType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getPath()));

        boolean useSetSafe = !(vvOut instanceof FixedWidthVector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        cg.addExpr(write);

        allocationVectors.add(vvOut);
      }
      ++index;
    }

    unionall = context.getImplementationClass(cg.getCodeGenerator());
    unionall.setup(context, inputBatch, this, transfers);
  }


  // The output table's column names always follow the left table,
  // where the output type is chosen based on DRILL's implicit casting rules
  private void inferOutputFieldsBothSide(final BatchSchema leftSchema, final BatchSchema rightSchema) {
//    outputFields = Lists.newArrayList();
    final Iterator<MaterializedField> leftIter = leftSchema.iterator();
    final Iterator<MaterializedField> rightIter = rightSchema.iterator();

    int index = 1;
    while (leftIter.hasNext() && rightIter.hasNext()) {
      MaterializedField leftField  = leftIter.next();
      MaterializedField rightField = rightIter.next();

      if (hasSameTypeAndMode(leftField, rightField)) {
        TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder().setMinorType(leftField.getType().getMinorType()).setMode(leftField.getDataMode());
        builder = Types.calculateTypePrecisionAndScale(leftField.getType(), rightField.getType(), builder);
        container.addOrGet(MaterializedField.create(leftField.getPath(), builder.build()), callBack);
      } else {
        // If the output type is not the same,
        // cast the column of one of the table to a data type which is the Least Restrictive
        TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder();
        if (leftField.getType().getMinorType() == rightField.getType().getMinorType()) {
          builder.setMinorType(leftField.getType().getMinorType());
          builder = Types.calculateTypePrecisionAndScale(leftField.getType(), rightField.getType(), builder);
        } else {
          List<TypeProtos.MinorType> types = Lists.newLinkedList();
          types.add(leftField.getType().getMinorType());
          types.add(rightField.getType().getMinorType());
          TypeProtos.MinorType outputMinorType = TypeCastRules.getLeastRestrictiveType(types);
          if (outputMinorType == null) {
            throw new DrillRuntimeException("Type mismatch between " + leftField.getType().getMinorType().toString() +
                " on the left side and " + rightField.getType().getMinorType().toString() +
                " on the right side in column " + index + " of UNION ALL");
          }
          builder.setMinorType(outputMinorType);
        }

        // The output data mode should be as flexible as the more flexible one from the two input tables
        List<TypeProtos.DataMode> dataModes = Lists.newLinkedList();
        dataModes.add(leftField.getType().getMode());
        dataModes.add(rightField.getType().getMode());
        builder.setMode(TypeCastRules.getLeastRestrictiveDataMode(dataModes));

        container.addOrGet(MaterializedField.create(leftField.getPath(), builder.build()), callBack);
      }
      ++index;
    }

    assert !leftIter.hasNext() && ! rightIter.hasNext() : "Mis-match of column count should have been detected when validating sqlNode at planning";
  }

  private void inferOutputFieldsOneSide(final BatchSchema schema) {
    for (MaterializedField field : schema) {
      container.addOrGet(field, callBack);
    }
  }

  private class BatchStatusWrappper {
    boolean prefetched;
    final RecordBatch batch;
    final int inputIndex;

    BatchStatusWrappper(boolean prefetched, RecordBatch batch, int inputIndex) {
      this.prefetched = prefetched;
      this.batch = batch;
      this.inputIndex = inputIndex;
    }
  }

  Pair<IterOutcome, RecordBatch> getNextBatch() {
    while (!batchStatusStack.isEmpty()) {
      BatchStatusWrappper topStatus = batchStatusStack.peek();

      if (topStatus.prefetched) {
        topStatus.prefetched = false;
        return Pair.of(IterOutcome.OK_NEW_SCHEMA, topStatus.batch);
      } else {
        IterOutcome outcome = next(topStatus.inputIndex, topStatus.batch);
        switch (outcome) {
          case OK:
          case OK_NEW_SCHEMA:
          case OUT_OF_MEMORY:
          case STOP:
            return Pair.of(outcome, topStatus.batch);
          case NONE:
            batchStatusStack.pop();
            break;
          default:
            throw new IllegalStateException(String.format("Unexpected state %s", outcome));
        }
      }
    }

    return Pair.of(IterOutcome.NONE, null);
  }


}
