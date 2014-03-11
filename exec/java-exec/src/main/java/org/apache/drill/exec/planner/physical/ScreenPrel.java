package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.planner.common.DrillScreenRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class ScreenPrel extends DrillScreenRelBase implements Prel {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScreenPrel.class);

  
  public ScreenPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
    super(Prel.DRILL_PHYSICAL, cluster, traits, child);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScreenPrel(getCluster(), traitSet, sole(inputs));
  }
  
  @Override  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    Screen s = new Screen(child.getPhysicalOperator(creator), null);
    creator.addPhysicalOperator(s);
    return s;
    // throw new IOException("ScreenPrel not supported yet!");
  }

}
