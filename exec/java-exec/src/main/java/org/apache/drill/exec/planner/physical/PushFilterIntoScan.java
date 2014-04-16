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

import java.io.IOException;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;



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
      //Convert "condition" from RexNode to LogicalExpression. 
      //TODO : add logic to check if conditions qualify for push-down.
      RexNode pushedCondition = filter.getCondition();  

      LogicalExpression conditionExp = ValueExpressions.getBit(true);           
      if (pushedCondition !=null) {
        conditionExp = DrillOptiq.toDrill(new DrillParseContext(), scan, pushedCondition);
      } else {
        return;  //no filter pushdown ==> No transformation. 
      }
              
      DrillTable drillTable = scan.getTable().unwrap(DrillTable.class);
      StoragePlugin plugin = drillTable.getPlugin();    
              
      try {
        GroupScan groupScan = plugin.getPhysicalScan(new JSONOptions(drillTable.getSelection()), scan.getColumns(), conditionExp);
        final DrillScanPrel newScan = new GroupScanPrel(scan.getCluster(), filter.getTraitSet(), groupScan, scan.getRowType());        
        call.transformTo(newScan);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
  }
  
}
