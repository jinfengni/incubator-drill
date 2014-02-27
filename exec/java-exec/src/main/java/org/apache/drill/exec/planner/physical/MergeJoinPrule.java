package org.apache.drill.exec.planner.physical;

import java.util.List;
import java.util.logging.Logger;

import net.hydromatic.optiq.util.BitSets;

import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillJoinRule;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.Lists;

public class MergeJoinPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new MergeJoinPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private MergeJoinPrule() {
    super(
        RelOptHelper.some(DrillJoinRel.class, RelOptHelper.any(RelNode.class), RelOptHelper.any(RelNode.class)),
        "Prel.MergeJoinPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);

    RelCollation collationLeft = getCollation(join.getLeftKeys());
    RelCollation collationRight = getCollation(join.getRightKeys());
    DrillDistributionTrait hashPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, null);
    
    final RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationLeft).plus(hashPartition);   
    final RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationRight).plus(hashPartition);
    
    final RelNode convertedLeft = convert(left, traitsLeft);
    final RelNode convertedRight = convert(right, traitsRight);
 
    try {          
      MergeJoinPrel newJoin = new MergeJoinPrel(join.getCluster(), traitsLeft, convertedLeft, convertedRight, join.getCondition(), join.getJoinType());
      call.transformTo(newJoin);
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }

  }
  
  private RelCollation getCollation(List<Integer> keys){    
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int key : keys) {
      fields.add(new RelFieldCollation(key));
    }
    return RelCollationImpl.of(fields);
  }

}
