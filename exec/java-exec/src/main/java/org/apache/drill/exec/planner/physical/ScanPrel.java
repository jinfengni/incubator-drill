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
import java.util.List;

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrel extends DrillScanRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);
    
  private ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl) {
    super(DRILL_PHYSICAL, cluster, traits, tbl);
  }

  private ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl, DrillTable drillTable) {
    super(DRILL_PHYSICAL, cluster, traits, tbl, drillTable);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanPrel(this.getCluster(), traitSet, this.getTable());
  }


  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new ScanPrel(this.getCluster(), this.getTraitSet(), this.getTable());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {    
    //TODO : need more accurate estimation based on groupScan's SIZE.
    GroupScan groupScan = this.getGroupScan();
    double dRows = groupScan.getSize().getRecordCount();
    double dCpu = groupScan.getSize().getRecordCount();
    double dIo = groupScan.getSize().getRecordCount();
    return this.getCluster().getPlanner().getCostFactory().makeCost(dRows, dCpu, dIo);    
//  return super.computeSelfCost(planner).multiplyBy(0.1);
  }
  
  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    GroupScan groupScan;
    groupScan = this.drillTable.getGroupScan();
    creator.addPhysicalOperator(groupScan);  
    return groupScan;
  }
 
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    GroupScan groupScan = this.getGroupScan();
    return super.explainTerms(pw).item("groupscan", groupScan.toString());
  }
  
  public static ScanPrel create(DrillScanRelBase old, RelTraitSet traitSets, RelOptTable tbl, DrillTable drillTable){
    return new ScanPrel(old.getCluster(), traitSets, tbl, drillTable);
  }

  public static ScanPrel create(DrillScanRelBase old, RelTraitSet traitSets, RelOptTable tbl){
    return new ScanPrel(old.getCluster(), traitSets, tbl);
  }

  public GroupScan getGroupScan() {
    try {
      return this.drillTable.getGroupScan();
    } catch (IOException e) {
      throw new RuntimeException("Failure getting group scan.", e);
    }   
  }
  
}
