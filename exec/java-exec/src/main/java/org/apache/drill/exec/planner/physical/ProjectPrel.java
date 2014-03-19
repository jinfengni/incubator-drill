package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class ProjectPrel extends DrillProjectRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectPrel.class);
  
  
  protected ProjectPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(DRILL_PHYSICAL, cluster, traits, child, exps, rowType);
  }

  public ProjectRelBase copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
    return new ProjectPrel(getCluster(), traitSet, input, exps, rowType);
  }


  @Override
  public PhysicalOPWithSV getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOPWithSV popsv = child.getPhysicalOperator(creator);
    
    PhysicalOperator childPOP = popsv.getPhysicalOperator();
    
    //Currently, Project only accepts "NONE". For other, requires SelectionVectorRemover
    if (!popsv.getSVMode().equals(SelectionVectorMode.NONE)) {
      childPOP = new SelectionVectorRemover(childPOP);
      creator.addPhysicalOperator(childPOP);
    }
    
    Project p = new Project(this.getProjectExpressions(creator.getContext()),  childPOP);
    creator.addPhysicalOperator(p);
    
    return new PhysicalOPWithSV(p, SelectionVectorMode.NONE);
  }


}
