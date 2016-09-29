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
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.proto.BitControl;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.store.parquet.ParquetRGFilterEvaluator.evalFilter;
import static org.apache.hadoop.hdfs.TestBlockStoragePolicy.conf;

public class TestParquetFilterPushDown extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetFilterPushDown.class);

  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
  private static FragmentContext fragContext;

  @BeforeClass
  public static void init() throws Exception {
    fragContext = new FragmentContext(bits[0].getContext(), BitControl.PlanFragment.getDefaultInstance(), null, bits[0].getContext().getFunctionImplementationRegistry());
  }

  @AfterClass
  public static void close() throws Exception {
    fragContext.close();
  }

  @Test
  public void testIntEqualPredicate() throws Exception {
    //    test("select * from dfs.`/drill/testdata/PF/orders` where o_orderkey = -1 or o_orderkey = -2");
    // test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_custkey < 2");
//    test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_totalprice < 1.0 ");
//    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate > date '2998-08-01'");
//      test("select * from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate/lineitem` where  L_SHIPDATE > date '2998-08-01'");
    test("select * from dfs.`/drill/testdata/PF/tpch-sf10-drill-bs10M_ob_shipdate/lineitem` where  cast(l_partkey as int) < -1");
    //    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate = cast(123456 as date)");
  }

  @Test
  public void testPF() throws Exception {


    final String filePath = String.format("%s/parquetFilterPush/intTbl/intTbl.parquet", TEST_RES_PATH);

    ParquetMetadata footer = getParquetMetaData(filePath);

    final String exprStr = " intCol =  100";

    final LogicalExpression filterExpr = parseExpr(exprStr);

    final Stopwatch watch = Stopwatch.createStarted();

    boolean canDrop = ParquetRGFilterEvaluator.evalFilter(filterExpr, footer,
        0, fragContext.getOptions(), fragContext);

    logger.debug("Took {} ms to evaluate the filter", watch.elapsed(TimeUnit.MILLISECONDS));

    System.out.print(footer.toString());
    System.out.print("canDrop = " + canDrop);
  }

  private LogicalExpression parseExpr(String expr) throws RecognitionException {
    final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ExprParser parser = new ExprParser(tokens);
    final ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

  private ParquetMetadata getParquetMetaData(String filePathStr) throws IOException{
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(filePathStr));
    return footer;
  }

}
