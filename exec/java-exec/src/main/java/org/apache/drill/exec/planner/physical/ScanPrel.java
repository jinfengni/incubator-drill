package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrel extends DrillScanRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);

  public ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl) {
    super(DRILL_PHYSICAL, cluster, traits, tbl);
  }

  
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return super.copy(traitSet, inputs);
  }


  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }


  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    StoragePlugin plugin = this.drillTable.getPlugin();
    GroupScan scan = plugin.getPhysicalScan(new JSONOptions(drillTable.getSelection()));
    creator.addPhysicalOperator(scan);
    return scan;    
  }
  
  
}
