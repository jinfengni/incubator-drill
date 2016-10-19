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

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.BitControl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestParquetFilterPushDown extends PlanTestBase {

  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
  private static FragmentContext fragContext;

  static FileSystem fs;

  @BeforeClass
  public static void initFSAndCreateFragContext() throws Exception {
    fragContext = new FragmentContext(bits[0].getContext(),
        BitControl.PlanFragment.getDefaultInstance(), null, bits[0].getContext().getFunctionImplementationRegistry());

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void close() throws Exception {
    fragContext.close();
    fs.close();
  }

  @Test
  // Test filter evaluation directly without go through SQL queries.
  public void testIntPredicateWithEval() throws Exception {
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

    //"nonExistCol" does not exist in the table. "AND" with a filter on exist column
    testParquetRowGroupFilterEval(footer, "intCol > 100 and nonExistCol = 100", true);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and nonExistCol = 100", true); // since nonExistCol = 100 -> Unknown -> could drop.
    testParquetRowGroupFilterEval(footer, "nonExistCol = 100 and intCol > 50", true); // since nonExistCol = 100 -> Unknown -> could drop.
    testParquetRowGroupFilterEval(footer, "intCol > 100 and nonExistCol < 'abc'", true);
    testParquetRowGroupFilterEval(footer, "nonExistCol < 'abc' and intCol > 100", true); // nonExistCol < 'abc' hit NumberException and is ignored, but intCol >100 will say "drop".
    testParquetRowGroupFilterEval(footer, "intCol > 50 and nonExistCol < 'abc'", false); // because nonExistCol < 'abc' hit NumberException and is ignored.

    //"nonExistCol" does not exist in the table. "OR" with a filter on exist column
    testParquetRowGroupFilterEval(footer, "intCol > 100 or nonExistCol = 100", true); // nonExistCol = 100 -> could drop.
    testParquetRowGroupFilterEval(footer, "nonExistCol = 100 or intCol > 100", true); // nonExistCol = 100 -> could drop.
    testParquetRowGroupFilterEval(footer, "intCol > 50 or nonExistCol < 100", false);
    testParquetRowGroupFilterEval(footer, "nonExistCol < 100 or intCol > 50", false);

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
  public void testIntPredicateAgainstAllNullColWithEval() throws Exception {
    // intAllNull.parquet has only one int column with all values being NULL.
    // column values statistics: num_nulls: 25, min/max is not defined
    final String filePath = String.format("%s/parquetFilterPush/intTbl/intAllNull.parquet", TEST_RES_PATH);
    ParquetMetadata footer = getParquetMetaData(filePath);

    testParquetRowGroupFilterEval(footer, "intCol = 100", true);
    testParquetRowGroupFilterEval(footer, "intCol = 0", true);
    testParquetRowGroupFilterEval(footer, "intCol = -100", true);

    testParquetRowGroupFilterEval(footer, "intCol > 10", true);
    testParquetRowGroupFilterEval(footer, "intCol >= 10", true);

    testParquetRowGroupFilterEval(footer, "intCol < 10", true);
    testParquetRowGroupFilterEval(footer, "intCol <= 10", true);
  }

  @Test
  public void testDatePredicateAgainstDrillCTASPost1_8WithEval() {

  }

  @Test
  // Test against parquet files from Drill CTAS post 1.8.0 release.
  public void testDatePredicateAgaistDrillCTASPost1_8() throws  Exception {
    String tableName = "order_ctas";

    try {
      deleteTableIfExists(tableName);

      test("use dfs_test.tmp");
      test(String.format("create table `%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03'", tableName));
      test(String.format("create table `%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and date '1992-01-06'", tableName));
      test(String.format("create table `%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and date '1992-01-09'", tableName));

      final String query1 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate = date '1992-01-01'";
      testParquetFilterPD(query1, 9, 1, false);

      final String query2 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate < date '1992-01-01'";
      testParquetFilterPD(query2, 0, 1, false);

      final String query3 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate between date '1992-01-01' and date '1992-01-03'";
      testParquetFilterPD(query3, 22, 1, false);

      final String query4 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate between date '1992-01-01' and date '1992-01-04'";
      testParquetFilterPD(query4, 33, 2, false);

      final String query5 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate between date '1992-01-01' and date '1992-01-06'";
      testParquetFilterPD(query5, 49, 2, false);

      final String query6 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate > date '1992-01-10'";
      testParquetFilterPD(query6, 0, 1, false);

      // Test parquet files with metadata cache files available.
      // Now, create parquet metadata cache files, and run the above queries again. Flag "usedMetadataFile" should be true.
      test(String.format("refresh table metadata %s", tableName));

      testParquetFilterPD(query1, 9, 1, true);

      testParquetFilterPD(query2, 0, 1, true);

      testParquetFilterPD(query3, 22, 1, true);

      testParquetFilterPD(query4, 33, 2, true);

      testParquetFilterPD(query5, 49, 2, true);

      testParquetFilterPD(query6, 0, 1, true);
    } finally {
      deleteTableIfExists(tableName);
    }
  }

  @Test
  public void testParquetFilterPDOptionsDisabled() throws Exception {
    String tableName = "order_ctas";

    try {
      deleteTableIfExists(tableName);

      test("alter session set `" + PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_KEY  + "` = false");

      test("use dfs_test.tmp");
      test(String.format("create table `%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03'", tableName));
      test(String.format("create table `%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and date '1992-01-06'", tableName));
      test(String.format("create table `%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and date '1992-01-09'", tableName));

      final String query1 = "select o_orderdate from dfs_test.tmp.order_ctas where o_orderdate = date '1992-01-01'";
      testParquetFilterPD(query1, 9, 3, false);

    } finally {
      test("alter session set `" + PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_KEY  + "` = " + PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING.getDefault().bool_val);
      deleteTableIfExists(tableName);
    }
  }

  @Test
  public void testDatePredicateAgainstCorruptedDateCol() throws Exception {
    // Table dateTblCorrupted is created by CTAS in drill 1.8.0. Per DRILL-4203, the date column is shifted by some value.
    // The CTAS are the following, then copy to drill test resource directory.
    //    create table dfs.tmp.`dateTblCorrupted/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03';
    //    create table dfs.tmp.`dateTblCorrupted/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and date '1992-01-06';
    //    create table dfs.tmp.`dateTblCorrupted/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and date '1992-01-09';

    final String query1 = String.format("select o_orderdate from dfs_test.`%s/parquetFilterPush/dateTblCorrupted` where o_orderdate = date '1992-01-01'", TEST_RES_PATH);
    testParquetFilterPD(query1, 9, 1, false);

    final String query2 = String.format("select o_orderdate from dfs_test.`%s/parquetFilterPush/dateTblCorrupted` where o_orderdate < date '1992-01-01'", TEST_RES_PATH);
    testParquetFilterPD(query2, 0, 1, false);

    final String query3 = String.format("select o_orderdate from dfs_test.`%s/parquetFilterPush/dateTblCorrupted` where o_orderdate between date '1992-01-01' and date '1992-01-03'", TEST_RES_PATH);
    testParquetFilterPD(query3, 22, 1, false);

    final String query4 = String.format("select o_orderdate from dfs_test.`%s/parquetFilterPush/dateTblCorrupted` where o_orderdate between date '1992-01-01' and date '1992-01-04'", TEST_RES_PATH);
    testParquetFilterPD(query4, 33, 2, false);

    final String query5 = String.format("select o_orderdate from dfs_test.`%s/parquetFilterPush/dateTblCorrupted` where o_orderdate between date '1992-01-01' and date '1992-01-06'", TEST_RES_PATH);
    testParquetFilterPD(query5, 49, 2, false);

    final String query6 = String.format("select o_orderdate from dfs_test.`%s/parquetFilterPush/dateTblCorrupted` where o_orderdate > date '1992-01-10'", TEST_RES_PATH);

    testParquetFilterPD(query6, 0, 1, false);
  }

  @Test
  public void test() throws Exception {
    //        test("select * from dfs.`/drill/testdata/PF/orders` where o_orderstatus2 = 100 and dir0 < 'Nov'");
    // test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_custkey < 2");
    //    test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_totalprice < 1.0 ");
    //    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate > date '2998-08-01'");
    //      test("select * from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate` where  dir0 = 'lineitem' and L_SHIPDATE > date '2998-08-01'");
    //    test("select * from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate/lineitem` where  cast(l_partkey as int) < -1");
    //    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate = cast(123456 as date)");
    //    test("select count(l_partkey) from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate` l where l.l_shipdate >= date '1994-08-01' \n"
    //        + "  and l.l_shipdate < date '1994-08-01' + interval '1' month");

    // intTbl.parquet has only one int column
    //    intCol : [0, 100].
    //    final String filePath = String.format("%s/parquetFilterPush/intTbl/intTbl.parquet", TEST_RES_PATH);
    //    ParquetMetadata footer = getParquetMetaData(filePath);

    //    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = -110.0", true);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Some test helper functions.
  //////////////////////////////////////////////////////////////////////////////////////////////////

  private void testParquetFilterPD(final String query, int expectedRowCount, int expectedNumFiles, boolean usedMetadataFile) throws Exception{
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=" + usedMetadataFile;

    testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[] {});
  }

  private void testParquetRowGroupFilterEval(final ParquetMetadata footer, final String exprStr,
      boolean canDropExpected) throws Exception{
    final LogicalExpression filterExpr = parseExpr(exprStr);
    testParquetRowGroupFilterEval(footer, 0, filterExpr, canDropExpected);
  }

  private void testParquetRowGroupFilterEval(final ParquetMetadata footer, final int rowGroupIndex,
      final LogicalExpression filterExpr, boolean canDropExpected) throws Exception {
    boolean canDrop = ParquetRGFilterEvaluator.evalFilter(filterExpr, footer, rowGroupIndex,
        fragContext.getOptions(), fragContext);
    Assert.assertEquals(canDropExpected, canDrop);
  }

  private ParquetMetadata getParquetMetaData(String filePathStr) throws IOException{
    Configuration fsConf = new Configuration();
    ParquetMetadata footer = ParquetFileReader.readFooter(fsConf, new Path(filePathStr));
    return footer;
  }

  private static void deleteTableIfExists(String tableName) {
    try {
      Path path = new Path(getDfsTestTmpSchemaLocation(), tableName);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (Exception e) {
      // ignore exceptions.
    }
  }

}
