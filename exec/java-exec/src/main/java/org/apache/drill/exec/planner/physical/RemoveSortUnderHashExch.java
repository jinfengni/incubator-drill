package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

import com.google.common.collect.ImmutableList;

/** Remove sort under HashToRandomExchange, since the exchange operator will destroy the order, making the sort operator completely useless. 
 */
public class RemoveSortUnderHashExch extends RelOptRule{
  public static final RelOptRule INSTANCE = new RemoveSortUnderHashExch();

  
  public RemoveSortUnderHashExch() {
    super(RelOptHelper.some(HashToRandomExchangePrel.class, RelOptHelper.any(SortPrel.class)), "Prel.RemoveSortUnderHashExch");    
  }
  
  public void onMatch(RelOptRuleCall call) {
    final HashToRandomExchangePrel exch = (HashToRandomExchangePrel) call.rel(0);
    final SortPrel sort = (SortPrel) call.rel(1);

    //final HashToRandomExchangePrel newExch = new HashToRandomExchangePrel(exch.getCluster(), exch.getTraitSet(), sort.getChild(), exch.);
    final HashToRandomExchangePrel newExch = (HashToRandomExchangePrel) exch.copy(exch.getTraitSet(), ImmutableList.of(sort.getChild()));
    
    call.transformTo(newExch);
  }
}
