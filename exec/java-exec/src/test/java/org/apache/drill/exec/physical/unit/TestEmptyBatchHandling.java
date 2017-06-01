/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.physical.unit;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.FlattenPOP;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestEmptyBatchHandling extends MiniPlanUnitTestBase{
  protected static DrillFileSystem fs;

  @BeforeClass
  public static void initFS() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    fs = new DrillFileSystem(conf);
  }

  @Test
  public void testEmptyJsonInput() throws Exception {
    RecordBatch scanBatch = createEmptyBatch();

    new MiniPlanTestBuilder()
        .root(scanBatch)
        .expectZeroBatch(true)
        .go();
  }

  @Test
  public void testProjectEmpty() throws Exception {
    final PhysicalOperator project = new Project(parseExprs("x+5", "x"), null);

    testEmptyBatchHandlingHelper(project);
  }

  @Test
  public void testFilterEmpty() throws Exception {
    final PhysicalOperator filter = new Filter(null, parseExpr("a=5"), 1.0f);

    testEmptyBatchHandlingHelper(filter);
  }

  @Test
  public void testHashAggEmpty() throws Exception {
    final PhysicalOperator hashAgg = new HashAggregate(null, parseExprs("a", "a"), parseExprs("sum(b)", "b_sum"), 1.0f);

    testEmptyBatchHandlingHelper(hashAgg);
  }

  @Test
  public void testStreamingAggEmpty() throws Exception {
    final PhysicalOperator hashAgg = new StreamingAggregate(null, parseExprs("a", "a"), parseExprs("sum(b)", "b_sum"), 1.0f);

    testEmptyBatchHandlingHelper(hashAgg);
  }

  @Test
  public void testSortEmpty() throws Exception {
    final PhysicalOperator sort = new ExternalSort(null,
        Lists.newArrayList(ordering("b", RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)), false);

    testEmptyBatchHandlingHelper(sort);
  }

  @Test
  public void testLimitEmpty() throws Exception {
    final PhysicalOperator limit = new Limit(null, 10, 5);

    testEmptyBatchHandlingHelper(limit);
  }

  @Test
  public void testFlattenEmpty() throws Exception {
    final PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("col1"));

    testEmptyBatchHandlingHelper(flatten);
  }

  @Test
  @Ignore("DRILL-5464: handle empty batch in UnionAll operator")
  public void testUnionEmptyBoth() throws Exception {
    final PhysicalOperator unionAll = new UnionAll(Collections.EMPTY_LIST); // Children list is provided through RecordBatch

    RecordBatch left = createEmptyBatch();
    RecordBatch right = createEmptyBatch();

    RecordBatch unionBatch = new PopBuilder()
        .physicalOperator(unionAll)
        .addInput(left)
        .addInput(right)
        .build();

    new MiniPlanTestBuilder()
        .root(unionBatch)
        .expectZeroBatch(true)
        .go();
  }

  @Test
  @Ignore("DRILL-5464: handle empty batch in UnionAll operator")
  public void testUnionLeftEmtpy() throws Exception {
    final PhysicalOperator unionAll = new UnionAll(Collections.EMPTY_LIST); // Children list is provided through RecordBatch

    RecordBatch left = createEmptyBatch();

    String file = FileUtils.getResourceAsFile("/tpchmulti/region/01.parquet").toURI().toString();

    RecordBatch scanBatch = new ParquetScanBuilder()
        .fileSystem(fs)
        .columnsToRead("R_REGIONKEY")
        .inputPaths(Lists.newArrayList(file))
        .build();

    RecordBatch projectBatch = new PopBuilder()
        .physicalOperator(new Project(parseExprs("R_REGIONKEY+10", "regionkey"), null))
        .addInput(scanBatch)
        .build();

    RecordBatch unionBatch = new PopBuilder()
        .physicalOperator(unionAll)
        .addInput(left)
        .addInput(projectBatch)
        .build();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("regionkey", TypeProtos.MinorType.BIGINT)
        .build();


    new MiniPlanTestBuilder()
        .root(unionBatch)
        .expectedSchema(expectedSchema)
        .baselineValues(10L)
        .baselineValues(11L)
        .go();
  }


  @Test
  // TODO: Fail.
  @Ignore("DRILL-5464: handle empty batch in Join operator")
  public void testInnerJoinEmptyBoth() throws Exception {
    RecordBatch left = createEmptyBatch();
    RecordBatch right = createEmptyBatch();

    RecordBatch joinBatch = new PopBuilder()
        .physicalOperator(new HashJoinPOP(null, null, Lists.newArrayList(joinCond("a", "EQUALS", "b")), JoinRelType.INNER))
        .addInput(left)
        .addInput(right)
        .build();

    new MiniPlanTestBuilder()
        .root(joinBatch)
        .expectZeroBatch(true)
        .go();
  }

  @Test
  // TODO: Fail.
  @Ignore("DRILL-5464: handle empty batch in Join operator")
  public void testLeftJoinEmptyBoth() throws Exception {
    RecordBatch left = createEmptyBatch();
    RecordBatch right = createEmptyBatch();

    RecordBatch joinBatch = new PopBuilder()
        .physicalOperator(new HashJoinPOP(null, null, Lists.newArrayList(joinCond("a", "EQUALS", "b")), JoinRelType.LEFT))
        .addInput(left)
        .addInput(right)
        .build();

    new MiniPlanTestBuilder()
        .root(joinBatch)
        .expectZeroBatch(true)
        .go();
  }

  @Test
  @Ignore ("DRILL-5327: A bug in UnionAll handling empty inputs from both sides")
  public void testUnionFilterAll() throws Exception {
    List<String> leftJsonBatches = Lists.newArrayList(
        "[{\"a\": 5, \"b\" : 1 }]");

    List<String> rightJsonBatches = Lists.newArrayList(
        "[{\"a\": 50, \"b\" : 10 }]");

    RecordBatch leftScan = new JsonScanBuilder()
        .jsonBatches(leftJsonBatches)
        .columnsToRead("a", "b")
        .build();

    RecordBatch leftFilter = new PopBuilder()
        .physicalOperator(new Filter(null, parseExpr("a < 0"), 1.0f))
        .addInput(leftScan)
        .build();

    RecordBatch rightScan = new JsonScanBuilder()
        .jsonBatches(rightJsonBatches)
        .columnsToRead("a", "b")
        .build();

    RecordBatch rightFilter = new PopBuilder()
        .physicalOperator(new Filter(null, parseExpr("a < 0"), 1.0f))
        .addInput(rightScan)
        .build();

    RecordBatch batch = new PopBuilder()
        .physicalOperator(new UnionAll(Collections.EMPTY_LIST)) // Children list is provided through RecordBatch
        .addInput(leftFilter)
        .addInput(rightFilter)
        .build();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", TypeProtos.MinorType.BIGINT)
        .addNullable("b", TypeProtos.MinorType.BIGINT)
        .withSVMode(BatchSchema.SelectionVectorMode.NONE)
        .build();

    new MiniPlanTestBuilder()
        .root(batch)
        .expectedSchema(expectedSchema)
        .go();
  }

  private void testEmptyBatchHandlingHelper(PhysicalOperator pop) throws Exception {
    final RecordBatch input = createEmptyBatch();

    RecordBatch projBatch = new PopBuilder()
        .physicalOperator(pop)
        .addInput(input)
        .build();

    new MiniPlanTestBuilder()
        .root(projBatch)
        .expectZeroBatch(true)
        .go();

  }

  private RecordBatch createEmptyBatch() throws Exception {
    String emptyFile = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();

    RecordBatch scanBatch = new JsonScanBuilder()
        .fileSystem(fs)
        .inputPaths(Lists.newArrayList(emptyFile))
        .build();

    return scanBatch;
  }

}
