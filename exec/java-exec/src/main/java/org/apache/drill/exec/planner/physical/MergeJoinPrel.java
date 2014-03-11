package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class MergeJoinPrel  extends DrillJoinRelBase implements Prel {
  private final List<Integer> leftKeys = new ArrayList<>();
  private final List<Integer> rightKeys = new ArrayList<>();

  /** Creates a MergeJoiPrel. */
  public MergeJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
    if (!remaining.isAlwaysTrue()) {
      throw new InvalidRelException("MergeJoinPrel only supports equi-join");
    }
  }

  
  @Override 
  public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType) {
    try {
      return new MergeJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new IOException("MergeJoinPrel not supported yet!");
  }

  public List<Integer> getLeftKeys() {
    return this.leftKeys;
  }
  
  public List<Integer> getRightKeys() {
    return this.rightKeys;
  }
  
}
