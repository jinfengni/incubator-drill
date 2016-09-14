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

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestParquetFilterPushDown extends BaseTestQuery{

  @Test
  public void testIntEqualPredicate() throws Exception {
    //    test("select * from dfs.`/drill/testdata/PF/orders` where o_orderkey = -1 or o_orderkey = -2");
    // test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_custkey < 2");
//    test("select * from dfs.`/drill/testdata/PF/orders_pt_custkey` where o_totalprice < 1.0 ");
    test("select * from dfs.`/drill/testdata/PF/orders` where  o_orderdate > date '1998-08-01'");
  }

}
