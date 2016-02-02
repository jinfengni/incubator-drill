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

package org.apache.drill.exec.planner.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import java.util.concurrent.TimeUnit;

public class DrillPushLimitToScanRule extends RelOptRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillPushLimitToScanRule.class);

  public static DrillPushLimitToScanRule INSTANCE = new DrillPushLimitToScanRule();

  private DrillPushLimitToScanRule() {
    super(RelOptHelper.some(DrillLimitRel.class, RelOptHelper.any(DrillScanRel.class)), "DrillPushLimitToScanRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    DrillScanRel scanRel = call.rel(1);
    return scanRel.getGroupScan() instanceof ParquetGroupScan;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    try {
      DrillLimitRel limitRel = call.rel(0);
      DrillScanRel scanRel = call.rel(1);

      int rowCountRequested = (int) limitRel.getRows();

      Pair<GroupScan, Boolean>  newGroupScanPair = ParquetGroupScan.filterParquetScanByLimit((ParquetGroupScan)(scanRel.getGroupScan()), rowCountRequested);
      if (! newGroupScanPair.right) {
        return;
      }

      DrillScanRel newScanRel = new DrillScanRel(scanRel.getCluster(),
          scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
          scanRel.getTable(),
          newGroupScanPair.left,
          scanRel.getRowType(),
          scanRel.getColumns(),
          scanRel.partitionFilterPushdown());

      RelNode newLimit = limitRel.copy(limitRel.getTraitSet(), ImmutableList.of((RelNode)newScanRel));

      call.transformTo(newLimit);
      logger.warn("Converted to a new DrillScanRel" + newScanRel.getGroupScan());
    } catch (Exception e) {
      logger.warn("Exception while using the pruned partitions.", e);
    } finally {
    }
  }
}
