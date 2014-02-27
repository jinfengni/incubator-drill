package org.apache.drill.exec.planner.physical;

import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

import com.google.common.collect.ImmutableList;

public class DrillDistributionTrait implements RelTrait {
  public static enum DistributionType {SINGLETON, HASH_DISTRIBUTED, RANGE_DISTRIBUTED, RANDOM_DISTRIBUTED, ANY};

  public static DrillDistributionTrait SINGLETON = new DrillDistributionTrait(DistributionType.SINGLETON);
  public static DrillDistributionTrait RANDOM_DISTRIBUTED = new DrillDistributionTrait(DistributionType.RANDOM_DISTRIBUTED);
  public static DrillDistributionTrait ANY = new DrillDistributionTrait(DistributionType.ANY);
  
  public static DrillDistributionTrait DEFAULT = SINGLETON;
  
  private DistributionType type;  
  private final ImmutableList<DistributionField> fields;
  
  private DrillDistributionTrait(DistributionType type) {
    assert (type == DistributionType.SINGLETON || type == DistributionType.RANDOM_DISTRIBUTED || type == DistributionType.ANY);
    this.type = type;
    this.fields = null;    
  }

  public DrillDistributionTrait(DistributionType type, ImmutableList<DistributionField> fields) {
    assert (type == DistributionType.HASH_DISTRIBUTED || type == DistributionType.RANGE_DISTRIBUTED);   
    this.type = type;
    this.fields = fields;
  }

  public boolean subsumes(RelTrait trait) {
    //if(trait == DEFAULT) return true;
    return this.equals(trait);
  }
  
  public RelTraitDef<DrillDistributionTrait> getTraitDef() {
    return DrillDistributionTraitDef.INSTANCE;
  }

  public DistributionType getType() {
    return this.type;
  }
  
  public int hashCode() {
    return  fields == null ? type.hashCode() : type.hashCode() | fields.hashCode() << 4 ;
  }
  
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DrillDistributionTrait) {
      DrillDistributionTrait that = (DrillDistributionTrait) obj;
      return this.fields == that.fields && this.type == that.type;
    }
    return false;
  }

  @Override
  public String toString() {
    return fields == null ? this.type.toString() : this.type.toString() + "(" + fields + ")";
  }

  
  public class DistributionField {
    /**
     * 0-based index of field being DISTRIBUTED.
     */
    private final int fieldId;
    
    public DistributionField (int fieldId) {
      this.fieldId = fieldId;
    }

    public boolean equals(Object obj) {
      if (!(obj instanceof DistributionField)) {
        return false;
      }
      DistributionField other = (DistributionField) obj;
      return this.fieldId == other.fieldId;
    }
    
    public int hashCode() {
      return this.fieldId;
    }
    
  }
  
}
