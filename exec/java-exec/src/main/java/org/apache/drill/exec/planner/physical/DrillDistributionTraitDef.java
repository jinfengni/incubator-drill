package org.apache.drill.exec.planner.physical;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;

public class DrillDistributionTraitDef extends RelTraitDef<DrillDistributionTrait>{
  public static final DrillDistributionTraitDef INSTANCE = new DrillDistributionTraitDef();
  
  private DrillDistributionTraitDef() {
    super();
  }
  
  public boolean canConvert(
      RelOptPlanner planner, DrillDistributionTrait fromTrait, DrillDistributionTrait toTrait) {
    return true;
  }  

  public Class<DrillDistributionTrait> getTraitClass(){
    return DrillDistributionTrait.class;
  }
  
  public DrillDistributionTrait getDefault() {
    return DrillDistributionTrait.DEFAULT;
  }

  public String getSimpleName() {
    return "DrillPartitionTrait";
  }

  // implement RelTraitDef
  public RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      DrillDistributionTrait toPartition,
      boolean allowInfiniteCostConverters) {
    
    DrillDistributionTrait currentPartition = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
    
    //Sournce and Target have the same trait.
    if (currentPartition.equals(toPartition)) {
      return rel;
    }
    
    //Source have the default trait -- "ANY". We do not want to convert from "ANY", since it's abstract. 
    //Source trait should be concreate : SINGLETON, HASH_DISTRIBUTED, etc.
    if (currentPartition.equals(DrillDistributionTrait.DEFAULT)) {
      return null;
    }
    
    switch(toPartition.getType()){
      case SINGLETON:
          // UnionExchange destroy the ordering property, therefore set it to EMPTY.
          //return new UnionExchangePrel(rel.getCluster(), rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition).plus(RelCollationImpl.EMPTY), rel);
        RelCollation collation2 = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        if (!collation2.equals(RelCollationImpl.EMPTY)) {
          return new SingleMergeExchangePrel(rel.getCluster(), rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition), rel);
        } else {
          return new UnionExchangePrel(rel.getCluster(), rel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition).plus(RelCollationImpl.EMPTY), rel);
        }  
      case HASH_DISTRIBUTED: 
        RelCollation collation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        RelNode exch = new HashToRandomExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition), rel);
        if (!collation.equals(RelCollationImpl.EMPTY)) {
          RelNode sort = new SortPrel(rel.getCluster(), exch.getTraitSet().plus(rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)), exch, collation);
          return sort;
        } else {
          return exch;
        }       
      case RANGE_DISTRIBUTED:
        return new OrderedPartitionExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toPartition), rel);
      default:
        return null;
    }

  }

}
