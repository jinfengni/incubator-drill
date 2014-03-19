package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.config.UnionExchange;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class UnionExchangePrel extends SingleRel implements Prel {

  public UnionExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
    //return planner.getCostFactory().makeCost(50, 50, 50);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new UnionExchangePrel(getCluster(), traitSet, sole(inputs));
  }
  
  public PhysicalOPWithSV getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOPWithSV popsv = child.getPhysicalOperator(creator);
    
    PhysicalOperator childPOP = popsv.getPhysicalOperator();
    
    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    if (!popsv.getSVMode().equals(SelectionVectorMode.NONE)) {
      childPOP = new SelectionVectorRemover(childPOP);
      creator.addPhysicalOperator(childPOP);
    }
   
    UnionExchange g = new UnionExchange(childPOP);
    creator.addPhysicalOperator(g);
    return new PhysicalOPWithSV(g,SelectionVectorMode.NONE) ;    
  }
  
}
