package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class HashPrel extends AggregateRelBase implements Prel{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashPrel.class);

  public HashPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, groupSet, aggCalls);
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException("HashPrel does not support DISTINCT aggregates");
      }
    }
    assert getConvention() == DRILL_PHYSICAL;
  }

  public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
    try {
      return new HashPrel(getCluster(), traitSet, input, getGroupSet(), aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }
  @Override
  public PhysicalOPWithSV getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    //Prel child = (Prel) this.getChild();
    //Project p = new Project(this.getProjectExpressions(creator.getContext()), child.getPhysicalOperator(creator));
    //return p;
    throw new IOException("HashPrel not supported yet!");
  }

}
