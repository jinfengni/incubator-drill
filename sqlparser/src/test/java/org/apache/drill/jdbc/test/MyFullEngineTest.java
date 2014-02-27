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
package org.apache.drill.jdbc.test;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;


public class MyFullEngineTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MyFullEngineTest.class);

  // Determine if we are in Eclipse Debug mode.
  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  // Set a timeout unless we're debugging.
  @Rule public TestRule globalTimeout = IS_DEBUG ? new TestName() : new Timeout(1000000);

  @Test
  @Ignore
  public void sqlTest() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("SELECT "  +
            "        _MAP['L_LINESTATUS'] as l_linestatus, " + 
            "        count(*) as count_order " +
            " FROM \"/Users/jni/work/tpc-h-parquet/lineitem/part-m-00000.parquet\" " +
            "GROUP BY _MAP['L_RETURNFLAG'], _MAP['L_LINESTATUS']").displayResults(50);
  }

  
  @Test
  @Ignore // since this is a specifically located file.
  public void fullSelectStarEngine() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select _MAP['d'] as d, _MAP['b'] as b from \"/tmp/parquet_test_file_many_types\" ").displayResults(50);
  }

  @Test
  @Ignore
  public void setCPRead() throws Exception {
    JdbcAssert.withFull("jsonl")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select count(*) from \"/tmp/scan.json\" ").displayResults(50);
  }
}
