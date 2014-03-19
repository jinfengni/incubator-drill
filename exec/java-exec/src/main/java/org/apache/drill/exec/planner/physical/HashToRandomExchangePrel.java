package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.fn.MathFunctions;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.expr.fn.impl.HashFunctions;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.config.SingleMergeExchange;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import com.google.common.collect.ImmutableList;

public class HashToRandomExchangePrel extends SingleRel implements Prel {

  private final List<DistributionField> fields;
  
  public HashToRandomExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<DistributionField> fields) {
    super(cluster, traitSet, input);
    this.fields = fields;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.1);    
    //return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToRandomExchangePrel(getCluster(), traitSet, sole(inputs), fields);
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

    HashToRandomExchange g = new HashToRandomExchange(childPOP, PrelUtil.getHashExpression(this.fields, getChild().getRowType()));
    creator.addPhysicalOperator(g);
    return new PhysicalOPWithSV(g, SelectionVectorMode.NONE);    
  }
  
  public List<DistributionField> getFields() {
    return this.fields;
  }
  
}
