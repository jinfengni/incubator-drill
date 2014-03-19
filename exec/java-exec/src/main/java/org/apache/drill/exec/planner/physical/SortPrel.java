package org.apache.drill.exec.planner.physical;

import java.io.IOException;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.SingleMergeExchange;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class SortPrel extends SortRel implements Prel {

  /** Creates a DrillSortRel. */
  public SortPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation) {
    super(cluster, traits, input, collation);
  }

  /** Creates a DrillSortRel with offset and fetch. */
  public SortPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
  }

  @Override
  public PhysicalOPWithSV getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOPWithSV popsv = child.getPhysicalOperator(creator);
    
    if (popsv.getSVMode().equals(SelectionVectorMode.FOUR_BYTE)) {
      throw new UnsupportedOperationException();
    }
    
    Sort g = new Sort(popsv.getPhysicalOperator(), PrelUtil.getOrdering(this.collation, getChild().getRowType()), false);
    
    creator.addPhysicalOperator(g);
    
    return new PhysicalOPWithSV(g, SelectionVectorMode.FOUR_BYTE);    
  }

  public SortPrel copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return new SortPrel(getCluster(), traitSet, newInput, newCollation);
  }
  
  
}
