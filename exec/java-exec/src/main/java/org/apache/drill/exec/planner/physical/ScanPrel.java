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

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import com.google.hive12.common.collect.Lists;

public class ScanPrel extends DrillScanRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);
  
  protected final RexNode condition;
  
  public ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl) {
    this(cluster, traits, tbl, null);
  }

  public ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl, RexNode condition) {
    super(DRILL_PHYSICAL, cluster, traits, tbl);
    this.condition = condition;
  }
  
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanPrel(this.getCluster(), traitSet, this.getTable(), this.condition);
  }


  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new ScanPrel(this.getCluster(), this.getTraitSet(), this.getTable(), this.condition);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(getSelectivity());
  }

  
  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    StoragePlugin plugin = this.drillTable.getPlugin();

    boolean containStar = false;
    final List<String> fields = getRowType().getFieldNames();
    List<SchemaPath> columns = Lists.newArrayList();
    for (String field : fields) {
      columns.add(new SchemaPath(field));

      if (field.startsWith("*")) {
        containStar = true;  
        break;
      }
    }
    
    if (containStar) {
      columns = null;
    }
    
    //Convert "condition" from RexNode to LogicalExpression. 
    LogicalExpression conditionExp = null;    
    if (this.condition !=null) {
      conditionExp = DrillOptiq.toDrill(new DrillParseContext(), this, this.condition);
    }

    //Push the projected columns + filter condition into scan operator, if any.    
    GroupScan scan = plugin.getPhysicalScan(new JSONOptions(drillTable.getSelection()), columns, conditionExp);      
    creator.addPhysicalOperator(scan);
    
    return scan;    
  }
 
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("condition", condition);
  }
  
  //Calculate the selectivity of filter condition "pushed" into this Scan OP.
  //For now, if there is no condition, selectivity = 1.0
  //Otherwise, selectivity = 0.5
  public double getSelectivity() {
    if (this.condition == null) {
      return 1.0d;
    } else {
      return 0.5d;
    }
  }
  
}
