package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

public class GroupScanPrel extends AbstractRelNode implements DrillScanPrel{

  private final GroupScan groupScan;
  private final RelDataType rowType;

  public GroupScanPrel(RelOptCluster cluster, RelTraitSet traits, GroupScan groupScan, RelDataType rowType) {
    super(cluster, traits);
    this.groupScan = groupScan;
    this.rowType = rowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new GroupScanPrel(this.getCluster(), traitSet, this.groupScan, this.rowType);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new GroupScanPrel(this.getCluster(), this.getTraitSet(), this.groupScan, this.rowType);
  }
  

  public GroupScan getGroupScan() {
    return this.groupScan;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    creator.addPhysicalOperator(groupScan);  
    return groupScan;
  }  
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {    
    //TODO : need more accurate estimation based on groupScan's SIZE.
    GroupScan groupScan = this.getGroupScan();
    double dRows = groupScan.getSize().getRecordCount();
    double dCpu = groupScan.getCost().getCpu();
    double dIo = groupScan.getCost().getDisk();
    double dMemory =groupScan.getCost().getMemory();
    return this.getCluster().getPlanner().getCostFactory().makeCost(dRows, dCpu, dIo);    
//  return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    GroupScan groupScan = this.getGroupScan();
    return super.explainTerms(pw).item("groupscan", groupScan.toString());
  }  
  
  public RelDataType deriveRowType() {
    return this.rowType;
  }
  
}
