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

package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

public class PushFilterIntoScan extends RelOptRule{
  
  public static final RelOptRule INSTANCE = new PushFilterIntoScan();

  private PushFilterIntoScan() {
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "PushFilterIntoScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final ScanPrel scan = (ScanPrel) call.rel(1);
    
    //Assume all the condition expressions in Filter op could be pushed down into Scan operator.
    final ScanPrel newScan = new ScanPrel(filter.getCluster(), filter.getTraitSet(), scan.getTable(), filter.getCondition());
    call.transformTo(newScan);
  }
  
}
