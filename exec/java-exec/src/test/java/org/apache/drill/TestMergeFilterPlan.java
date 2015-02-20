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

import org.junit.Test;

public class TestMergeFilterPlan extends PlanTestBase {

  @Test
  public void testDRILL_FilterMerge() throws Exception {
    String viewDDL = "create or replace view MyViewWithFilter as " +
        " SELECT  first_name, " +
        "         last_name, " +
        "         full_name, " +
        "         salary, " +
        "         employee_id, " +
        "         store_id, " +
        "         position_id, " +
        "         position_title, " +
        "         education_level " +
        " FROM cp.`employee.json` " +
        " WHERE position_id in (1, 2, 3 ) " ;

    String querySQL =  " select dat.store_id\n" +
        "      , sum(dat.store_cost) as total_cost\n" +
        " from (\n" +
        "  select store_id, position_id\n" +
        "            , sum( salary) as store_cost\n" +
        "       from MyViewWithFilter \n" +
        " where full_name in ( select n_name\n" +
        "                      from cp.`tpch/nation.parquet`)\n" +
        "  and  education_level = 'GRADUATE DEGREE' " +
        "  and position_id in ( select position_id \n" +
        "                       from MyViewWithFilter\n" +
        "                        where position_title like '%VP%'\n" +
        "                      )\n" +
        "  group by store_id, position_id\n" +
        ") dat\n" +
        "group by dat.store_id\n" +
        "order by dat.store_id";

    String expectedPattern1 = "Filter(condition=[AND(OR(=($0, 1), =($0, 2), =($0, 3)), =(CAST($4):ANY NOT NULL, 'GRADUATE DEGREE'))])";
    String expectedPattern2 = "Filter(condition=[AND(OR(=($0, 1), =($0, 2), =($0, 3)), LIKE(CAST($1):ANY NOT NULL, '%VP%'))])";
    String excludedPattern = "Filter(condition=[OR(=($0, 1), =($0, 2), =($0, 3))])";

    test("use dfs.tmp");

    test(viewDDL);

    testPlanSubstrPatterns(querySQL,
        new String[]{expectedPattern1, expectedPattern2}, new String[]{excludedPattern});

    test("drop view MyViewWithFilter ");
  }

  /**
   * Runs an explain plan query and check for expected substring patterns (in optiq
   * text format), also ensure excluded patterns are not found. Either list can
   * be empty or null to skip that part of the check.
   *
   * This is different from testPlanMatchingPatterns in that this one use substring contains,
   * in stead of regex pattern matching. This one is useful when the pattern contains
   * many regex reserved chars, and you do not want to put the escape char.
   *
   * See the convenience methods for passing a single string in either the
   * excluded list, included list or both.
   *
   * @param query - an explain query, this method does not add it for you
   * @param expectedPatterns - list of patterns that should appear in the plan
   * @param excludedPatterns - list of patterns that should not appear in the plan
   * @throws Exception - if an inclusion or exclusion check fails, or the
   *                     planning process throws an exception
   */
  private void testPlanSubstrPatterns(String query, String[] expectedPatterns, String[] excludedPatterns) throws Exception {
    String plan = getPlanInString("EXPLAIN PLAN for " + normalizeQuery(query), OPTIQ_FORMAT);

    // Check and make sure all expected patterns are in the plan
    if (expectedPatterns != null) {
      for (String s : expectedPatterns) {
        assert plan.contains(s) : EXPECTED_NOT_FOUND + s;
      }
    }

    // Check and make sure all excluded patterns are not in the plan
    if (excludedPatterns != null) {
      for (String s : excludedPatterns) {
        assert ! plan.contains(s) : UNEXPECTED_FOUND + s;
      }
    }
  }

}
