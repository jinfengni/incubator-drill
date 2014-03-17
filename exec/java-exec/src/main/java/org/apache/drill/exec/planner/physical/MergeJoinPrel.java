package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import com.beust.jcommander.internal.Lists;

public class MergeJoinPrel  extends DrillJoinRelBase implements Prel {
  private final List<Integer> leftKeys = new ArrayList<>();
  private final List<Integer> rightKeys = new ArrayList<>();

  private final JoinCondition[] joinConditions; // Drill's representation of join conditions
  
  /** Creates a MergeJoiPrel. */
  public MergeJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType, JoinCondition[] joinConditions) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
    if (!remaining.isAlwaysTrue()) {
      throw new InvalidRelException("MergeJoinPrel only supports equi-join");
    }
    this.joinConditions = joinConditions;
  }

  
  @Override 
  public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType) {
    try {
      return new MergeJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType, this.getJoinConditions());
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override  
  public PhysicalOPWithSV getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    PhysicalOperator leftPop = ((Prel) getLeft()).getPhysicalOperator(creator).getPhysicalOperator();
    PhysicalOperator rightPop = ((Prel) getRight()).getPhysicalOperator(creator).getPhysicalOperator();
    JoinRelType jtype = this.getJoinType();
    
    MergeJoinPOP mjoin = new MergeJoinPOP(leftPop, rightPop, Arrays.asList(joinConditions), jtype);
    creator.addPhysicalOperator(mjoin);
   
    return new PhysicalOPWithSV(mjoin, SelectionVectorMode.FOUR_BYTE);
  }

  public List<Integer> getLeftKeys() {
    return this.leftKeys;
  }
  
  public List<Integer> getRightKeys() {
    return this.rightKeys;
  }
  
  public JoinCondition[] getJoinConditions() {
    return joinConditions;
  }
}
