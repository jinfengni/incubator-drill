/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

public abstract class ParquetPushDownFilter extends StoragePluginOptimizerRule {

  public static RelOptRule getFilterOnProject(OptimizerRulesContext optimizerRulesContext) {
    return new ParquetPushDownFilter(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
        "ParquetPushDownFilter:Filter_On_Project", optimizerRulesContext) {

      @Override
      public boolean matches(RelOptRuleCall call) {
        if (!enabled) {
          return false;
        }
        final DrillScanRel scan = call.rel(2);
        if (scan.getGroupScan() instanceof ParquetGroupScan) {
          return super.matches(call);
        }
        return false;
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final DrillFilterRel filterRel = call.rel(0);
        final DrillProjectRel projectRel = call.rel(1);
        final DrillScanRel scanRel = call.rel(2);
        doOnMatch(call, filterRel, projectRel, scanRel);
      }

    };
  }

  public static StoragePluginOptimizerRule getFilterOnScan(OptimizerRulesContext optimizerContext) {
    return new ParquetPushDownFilter(
        RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
        "ParquetPushDownFilter:Filter_On_Scan", optimizerContext) {

      @Override
      public boolean matches(RelOptRuleCall call) {
        if (!enabled) {
          return false;
        }
        final DrillScanRel scan = call.rel(1);
        if (scan.getGroupScan() instanceof ParquetGroupScan) {
          return super.matches(call);
        }
        return false;
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final DrillFilterRel filterRel = call.rel(0);
        final DrillScanRel scanRel = call.rel(1);
        doOnMatch(call, filterRel, null, scanRel);
      }
    };
  }

  // private final boolean useNewReader;
  protected final boolean enabled;
  protected final OptimizerRulesContext optimizerContext;

  private ParquetPushDownFilter(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.optimizerContext = optimizerContext;
    this.enabled = true; // context.getPlannerSettings().isParquetFilterPushEnabled();
    // this.useNewReader = context.getPlannerSettings()getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val;
  }

  protected void doOnMatch(RelOptRuleCall call, DrillFilterRel filter, DrillProjectRel project, DrillScanRel scan) {
    ParquetGroupScan groupScan = (ParquetGroupScan) scan.getGroupScan();
    if (groupScan.getFilter() != null && !groupScan.getFilter().equals(ValueExpressions.BooleanExpression.TRUE)) {
      return;
    }

    RexNode condition = null;
    if (project == null) {
      condition = filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(filter.getCondition(), project);
    }

    if (condition == null || condition.equals(ValueExpressions.BooleanExpression.TRUE)) {
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);

    final GroupScan newGroupScan = groupScan.applyFilter(conditionExp,optimizerContext, optimizerContext.getFunctionRegistry(), optimizerContext.getPlannerSettings().getOptions());

    if (newGroupScan == null ) {
      return;
    }

    final DrillScanRel newScanRel = new DrillScanRel(scan.getCluster(),
        scan.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
        scan.getTable(),
        newGroupScan,
        scan.getRowType(),
        scan.getColumns(),
        scan.partitionFilterPushdown());

//        sca
//        DrillScanRel.create(scan, filter.getTraitSet(),
//        newGroupScan, scan.getRowType());

    RelNode inputRel = newScanRel;

    if (project != null) {
      inputRel = project.copy(project.getTraitSet(), ImmutableList.of(inputRel));
    }

    // Normally we could eliminate the filter if all expressions were pushed down;
    // however, the Parquet filter implementation is type specific (whereas Drill is not)
    final RelNode newFilter = filter.copy(filter.getTraitSet(), ImmutableList.of(inputRel));
    call.transformTo(newFilter);
  }
}
