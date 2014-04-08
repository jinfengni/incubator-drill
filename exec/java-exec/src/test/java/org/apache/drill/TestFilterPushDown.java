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

package org.apache.drill;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestFilterPushDown extends PopUnitTestBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFilterPushDown.class);
  
  static Drillbit bit1;  
  static DrillSqlWorker worker;
   
  //Initialize the Drillbit and DrillSqlWorker.
  @BeforeClass
  public static void setUp() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    bit1 = new Drillbit(CONFIG, serviceSet);
    bit1.run();
    QueryContext qContext = new QueryContext(new UserSession(null, null), QueryId.getDefaultInstance(), bit1.getContext());
    worker = new DrillSqlWorker(qContext);
  }
    
  @Test
  public void testFilter() throws Exception{    
    testPhysicalPlan("SELECT\n" + 
        "  nations.N_NAME \n" + 
        "FROM\n" + 
        "  dfs.`[WORKING_PATH]/../../sample-data/nation.parquet` nations\n" + 
        "  WHERE nations.N_REGIONKEY = 12345"
        );
  }

  // This method will take a SQL string statement, and a list of expected columns (for join).
  // Get the physical plan from DrillSqlWorker and serialize the physical plan into string.
  // Verify all the expected columns are contained in the physical plan string. 
  private void testPhysicalPlan(String sql, String... expectedSubstrs) throws Exception{   
    
    sql = sql.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    
    QueryContext qContext = new QueryContext(new UserSession(null, null), QueryId.getDefaultInstance(), bit1.getContext());
    PhysicalPlan plan = worker.getPhysicalPlan(sql, qContext);    
    String planStr = qContext.getConfig().getMapper().writeValueAsString(plan);
    
    System.out.print(qContext.getConfig().getMapper().writeValueAsString(plan));
      
    for (String colNames : expectedSubstrs) {
      assertTrue(planStr.contains(colNames));
    }
    
    Thread.sleep(2000);
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    bit1.close();
  }
}
