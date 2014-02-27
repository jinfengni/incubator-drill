package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.common.BaseScreenRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ScreenPrule extends RelOptRule{
  public static final RelOptRule INSTANCE = new ScreenPrule();

  
  public ScreenPrule() {
    super(RelOptHelper.some(DrillScreenRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)), "Prel.ScreenPrule");    
  }
  
  @Override
  public void onMatch(RelOptRuleCall call) {
    final BaseScreenRel screen = (BaseScreenRel) call.rel(0);
    final RelNode input = call.rel(1);
    
    final RelTraitSet traits = screen.getTraitSet().replace(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    final RelNode convertedInput = convert(input, traits);
    BaseScreenRel newScreen = new ScreenPrel(screen.getCluster(), screen.getTraitSet().replace(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON), convertedInput);
    call.transformTo(newScreen);
  }


}
