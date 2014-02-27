package org.apache.drill.exec.planner.physical;

import java.util.Collections;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;

public class DrillMuxModeDef extends RelTraitDef<DrillMuxMode>{
  public static final DrillMuxModeDef INSTANCE = new DrillMuxModeDef();
  
  private DrillMuxModeDef() {
    super();
  }
  
  public boolean canConvert(
      RelOptPlanner planner, DrillMuxMode fromTrait, DrillMuxMode toTrait) {
    return true;
  }  

  public Class<DrillMuxMode> getTraitClass(){
    return DrillMuxMode.class;
  }
  
  public DrillMuxMode getDefault() {
    return DrillMuxMode.DEFAULT;
  }

  public String getSimpleName() {
    return "DrillMuxMode";
  }

  // implement RelTraitDef
  public RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      DrillMuxMode toMuxMode,
      boolean allowInfiniteCostConverters) {
    
    DrillMuxMode currentMuxMode = rel.getTraitSet().getTrait(DrillMuxModeDef.INSTANCE);
    
    if (currentMuxMode.equals(toMuxMode)) {
      return rel;
    }
    
    switch(toMuxMode.getMode()){
      case NO_MUX: 
        return rel;
      case SIMPLEX:
      if(currentMuxMode.getMode() == DrillMuxMode.MuxMode.SIMPLEX){
        // the rel trait is DEFAULT, we want it to be SIMPLEX.  Just copy with new trait since they are interchangeable.
        return rel.copy(rel.getTraitSet().plus(DrillMuxMode.SIMPLEX), Collections.singletonList(rel));
      }else{
        // input is multiplex so we need to first add union exchange to convert to simplex.
        return new UnionExchangePrel(rel.getCluster(), rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillMuxMode.SIMPLEX), rel);
      }
      case MULTIPLEX: 
        RelCollation collation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        RelNode exch = new HashToRandomExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillMuxMode.MULTIPLEX), rel);
        if (!collation.equals(RelCollationImpl.EMPTY)) {
          RelNode sort = new SortPrel(rel.getCluster(), exch.getTraitSet().plus(rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)), exch, collation);
          return sort;
        } else {
          return exch;
        }
        
      default:
        return null;
    }

  }
 
}
