package org.apache.drill.exec.planner.physical;

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class ProjectPrel extends ProjectRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectPrel.class);
  
  
  protected ProjectPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(cluster, traits, child, exps, rowType, Flags.BOXED);
    assert getConvention() == DRILL_PHYSICAL;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ProjectPrel(getCluster(), traitSet, sole(inputs), new ArrayList<RexNode>(exps), rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }


}
