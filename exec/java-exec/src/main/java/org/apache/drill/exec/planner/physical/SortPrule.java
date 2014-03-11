package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

/**
 * 
 * Rule that converts a logical {@link DrillSortRel} to a physical sort.  Convert from Logical Sort into Physical Sort. 
 * For Logical Sort, it requires one single data stream as the output. 
 *
 */
public class SortPrule extends RelOptRule{
  public static final RelOptRule INSTANCE = new SortPrule();

  private SortPrule() {
    super(RelOptHelper.some(DrillSortRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)), "Prel.SortPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillSortRel sort = (DrillSortRel) call.rel(0);
    final RelNode input = call.rel(1);
    
    // Keep the collation in logical sort. Convert input into a RelNode with 1) this collation, 2) Physical, 3) single output stream.
    
    final RelTraitSet traits = sort.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    
    final RelNode convertedInput = convert(input, traits);
    call.transformTo(convertedInput);  // transform logical "sort" into convertedInput ("sort" may not needed in some cases).
    
  }

}
