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
import org.junit.Test;

import static org.junit.Assert.assertTrue;

// Test the optimizer plan in terms of project pushdown. 
// When a query refers to a subset of columns in a table, optimizer should push the list
// of refereed columns to the SCAN operator, so that SCAN operator would only retrieve
// the column values in the subset of columns. 

public class TestProjectPushDown extends PlanningBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestProjectPushDown.class);
    
  @Test
  public void testGroupBy() throws Exception{
    String expectedColNames = " \"columns\" : [ \"`marital_status`\" ]";
    testPhysicalPlan("select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status", expectedColNames);
  }

  @Test
  public void testOrderBy() throws Exception{
    String expectedColNames = "\"columns\" : [ \"`employee_id`\", \"`full_name`\", \"`first_name`\", \"`last_name`\" ]";
    testPhysicalPlan("select employee_id , full_name, first_name , last_name "
        + "from cp.`employee.json` order by first_name, last_name", 
        expectedColNames);
  }
  
  
  @Test
  public void testJoin() throws Exception{
    String expectedColNames1 = "\"columns\" : [ \"`N_REGIONKEY`\", \"`N_NAME`\" ]";
    String expectedColNames2 = "\"columns\" : [ \"`R_REGIONKEY`\", \"`R_NAME`\" ]";
    
    testPhysicalPlan("SELECT\n" + 
        "  nations.N_NAME,\n" + 
        "  regions.R_NAME\n" + 
        "FROM\n" + 
        "  dfs.`[WORKING_PATH]/../../sample-data/nation.parquet` nations\n" + 
        "JOIN\n" + 
        "  dfs.`[WORKING_PATH]/../../sample-data/region.parquet` regions\n" + 
        "  on nations.N_REGIONKEY = regions.R_REGIONKEY",
        expectedColNames1,expectedColNames2);
  }

  // This method will take a SQL string statement, and a list of expected columns (for join).
  // Get the physical plan from DrillSqlWorker and serialize the physical plan into string.
  // Verify all the expected columns are contained in the physical plan string. 
  private void testPhysicalPlan(String sql, String... expectedSubstrs) throws Exception{   
    
    sql = sql.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    
    String planStr = testSqlPlan(sql);
    
    System.out.print("Physical plan :"  + planStr);
    for (String colNames : expectedSubstrs) {
      assertTrue(planStr.contains(colNames));
    }
    
    Thread.sleep(2000);
  }
  
}
