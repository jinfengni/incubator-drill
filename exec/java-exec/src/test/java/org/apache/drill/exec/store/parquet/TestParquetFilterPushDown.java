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
package org.apache.drill.exec.store.parquet;

import com.google.common.base.Stopwatch;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.TestBlockStoragePolicy.conf;

public class TestParquetFilterPushDown extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetFilterPushDown.class);

  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
  private static FragmentContext fragContext;

  @BeforeClass
  public static void init() throws Exception {
    fragContext = new FragmentContext(bits[0].getContext(),
        BitControl.PlanFragment.getDefaultInstance(), null, bits[0].getContext().getFunctionImplementationRegistry());
  }

  @AfterClass
  public static void close() throws Exception {
    fragContext.close();
  }

  @Test
  public void testIntEqualPredicate() throws Exception {
//        test("select * from dfs.`/drill/testdata/PF/orders` where o_orderstatus2 = 100 and dir0 < 'Nov'");
    // test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_custkey < 2");
//    test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_totalprice < 1.0 ");
//    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate > date '2998-08-01'");
//      test("select * from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate` where  dir0 = 'lineitem' and L_SHIPDATE > date '2998-08-01'");
//    test("select * from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate/lineitem` where  cast(l_partkey as int) < -1");
    //    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate = cast(123456 as date)");
//    test("select l_partkey from dfs.`/drill/testdata/tpch01/parquet/lineitem` l where l.l_shipdate >= date '1994-08-01' \n"
//        + "  and l.l_shipdate < date '1994-08-01' + interval '1' month");
  }

  @Test
  public void test() throws Exception {
    // intTbl.parquet has only one int column
    //    intCol : [0, 100].
    final String filePath = String.format("%s/parquetFilterPush/intTbl/intTbl.parquet", TEST_RES_PATH);
    ParquetMetadata footer = getParquetMetaData(filePath);

    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = -110.0", true);
  }

    @Test
  public void testIntPredicate() throws Exception {
    // intTbl.parquet has only one int column
    //    intCol : [0, 100].
    final String filePath = String.format("%s/parquetFilterPush/intTbl/intTbl.parquet", TEST_RES_PATH);
    ParquetMetadata footer = getParquetMetaData(filePath);

    testParquetRowGroupFilterEval(footer, "intCol = 100", false);
    testParquetRowGroupFilterEval(footer, "intCol = 0", false);
    testParquetRowGroupFilterEval(footer, "intCol = 50", false);

    testParquetRowGroupFilterEval(footer, "intCol = -1", true);
    testParquetRowGroupFilterEval(footer, "intCol = 101", true);

    testParquetRowGroupFilterEval(footer, "intCol > 100", true);
    testParquetRowGroupFilterEval(footer, "intCol > 99", false);

    testParquetRowGroupFilterEval(footer, "intCol >= 100", false);
    testParquetRowGroupFilterEval(footer, "intCol >= 101", true);

    testParquetRowGroupFilterEval(footer, "intCol < 100", false);
    testParquetRowGroupFilterEval(footer, "intCol < 1", false);
    testParquetRowGroupFilterEval(footer, "intCol < 0", true);

    testParquetRowGroupFilterEval(footer, "intCol <= 100", false);
    testParquetRowGroupFilterEval(footer, "intCol <= 1", false);
    testParquetRowGroupFilterEval(footer, "intCol <= 0", false);
    testParquetRowGroupFilterEval(footer, "intCol <= -1", true);

    // "and"
    testParquetRowGroupFilterEval(footer, "intCol > 100 and intCol  < 200", true);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and intCol < 200", false);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and intCol > 200", true); // essentially, intCol > 200

    // "or"
    testParquetRowGroupFilterEval(footer, "intCol = 150 or intCol = 160", true);
    testParquetRowGroupFilterEval(footer, "intCol = 50 or intCol = 160", false);

    //"nonExistCol" does not exist in the table.
    testParquetRowGroupFilterEval(footer, "intCol > 100 and nonExistCol = 100", true);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and nonExistCol = 100", false); // TODO : should be true since nonExistCol = 100 -> Unknown -> could drop.
//
    testParquetRowGroupFilterEval(footer, "intCol > 100 and nonExistCol < 'abc'", true);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and nonExistCol < 'abc'", false); // because nonExistCol < 'abc' hit NumberException and is ignored.

    testParquetRowGroupFilterEval(footer, "intCol > 100 or nonExistCol = 100", false); // TODO: should be true
    testParquetRowGroupFilterEval(footer, "intCol > 50 or nonExistCol < 200", false);

    // cast function on column side (LHS)
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 100", false);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 0", false);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 50", false);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 101", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = -1", true);

    // cast function on constant side (RHS)
    testParquetRowGroupFilterEval(footer, "intCol = cast(100 as bigint)", false);
    testParquetRowGroupFilterEval(footer, "intCol = cast(0 as bigint)", false);
    testParquetRowGroupFilterEval(footer, "intCol = cast(50 as bigint)", false);
    testParquetRowGroupFilterEval(footer, "intCol = cast(101 as bigint)", true);
    testParquetRowGroupFilterEval(footer, "intCol = cast(-1 as bigint)", true);

    // cast into float4/float8
    testParquetRowGroupFilterEval(footer, "cast(intCol as float4) = cast(101.0 as float4)", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float4) = cast(-1.0 as float4)", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float4) = cast(1.0 as float4)", false);

    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = 101.0", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = -1.0", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = 1.0", false);

  }

  @Test
  public void testIntPredicateAgainstAllNullCol() throws Exception {
    // intAllNull.parquet has only one int column with all values being NULL.
    // column values statistics: num_nulls: 25, min/max is not defined
    final String filePath = String.format("%s/parquetFilterPush/intTbl/intAllNull.parquet", TEST_RES_PATH);
    ParquetMetadata footer = getParquetMetaData(filePath);

    testParquetRowGroupFilterEval(footer, "intCol = 100", true);
    testParquetRowGroupFilterEval(footer, "intCol = 0", true);
    testParquetRowGroupFilterEval(footer, "intCol = -100", true);
  }


  private void testParquetRowGroupFilterEval(final ParquetMetadata footer, final String exprStr,
      boolean canDropExpected) throws Exception{
    final LogicalExpression filterExpr = parseExpr(exprStr);
    testParquetRowGroupFilterEval(footer, 0, filterExpr, canDropExpected);
  }

  private void testParquetRowGroupFilterEval(final ParquetMetadata footer, final int rowGroupIndex,
      final LogicalExpression filterExpr, boolean canDropExpected) throws Exception {
    final Stopwatch watch = Stopwatch.createStarted();

    boolean canDrop = ParquetRGFilterEvaluator.evalFilter(filterExpr, footer, rowGroupIndex,
        fragContext.getOptions(), fragContext);
    logger.debug("Took {} ms to evaluate the filter", watch.elapsed(TimeUnit.MILLISECONDS));
    Assert.assertEquals(canDropExpected, canDrop);
  }

  private ParquetMetadata getParquetMetaData(String filePathStr) throws IOException{
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(filePathStr));
    return footer;
  }

}
