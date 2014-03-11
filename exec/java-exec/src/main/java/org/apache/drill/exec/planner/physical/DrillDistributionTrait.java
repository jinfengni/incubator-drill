package org.apache.drill.exec.planner.physical;

import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

import com.google.common.collect.ImmutableList;

public class DrillDistributionTrait implements RelTrait {
  public static enum DistributionType {SINGLETON, HASH_DISTRIBUTED, RANGE_DISTRIBUTED, RANDOM_DISTRIBUTED,
                                       ROUND_ROBIN_DISTRIBUTED, BROADCAST_DISTRIBUTED, ANY};

  public static DrillDistributionTrait SINGLETON = new DrillDistributionTrait(DistributionType.SINGLETON);
  public static DrillDistributionTrait RANDOM_DISTRIBUTED = new DrillDistributionTrait(DistributionType.RANDOM_DISTRIBUTED);
  public static DrillDistributionTrait ANY = new DrillDistributionTrait(DistributionType.ANY);
  
  public static DrillDistributionTrait DEFAULT = ANY;
  
  private DistributionType type;  
  private final ImmutableList<DistributionField> fields;
  
  private DrillDistributionTrait(DistributionType type) {
    assert (type == DistributionType.SINGLETON || type == DistributionType.RANDOM_DISTRIBUTED || type == DistributionType.ANY
            || type == DistributionType.ROUND_ROBIN_DISTRIBUTED || type == DistributionType.BROADCAST_DISTRIBUTED);
    this.type = type;
    this.fields = ImmutableList.<DistributionField>of();
  }

  public DrillDistributionTrait(DistributionType type, ImmutableList<DistributionField> fields) {
    assert (type == DistributionType.HASH_DISTRIBUTED || type == DistributionType.RANGE_DISTRIBUTED);   
    this.type = type;
    this.fields = fields;
  }

  public boolean subsumes(RelTrait trait) {

    if (trait instanceof DrillDistributionTrait) {
      DistributionType requiredDist = ((DrillDistributionTrait) trait).getType();
      if (requiredDist == DistributionType.ANY) {
        return true;
      }

      if (this.type == DistributionType.HASH_DISTRIBUTED) {
        if (requiredDist == DistributionType.HASH_DISTRIBUTED) {
          ImmutableList<DistributionField> thisFields = this.fields;
          ImmutableList<DistributionField> requiredFields = ((DrillDistributionTrait)trait).getFields();

          assert(thisFields.size() > 0 && requiredFields.size() > 0);

          // A subset of the required distribution columns can satisfy (subsume) the requirement
          // e.g: required distribution: {a, b, c} 
          // Following can satisfy the requirements: {a}, {b}, {c}, {a, b}, {b, c}, {a, c} or {a, b, c}
          return (requiredFields.containsAll(thisFields));
        }
        else if (requiredDist == DistributionType.RANDOM_DISTRIBUTED) {
          return true; // hash distribution subsumes random distribution and ANY distribution 
        }
      }
    }

    return this.equals(trait);
  }
  
  public RelTraitDef<DrillDistributionTrait> getTraitDef() {
    return DrillDistributionTraitDef.INSTANCE;
  }

  public DistributionType getType() {
    return this.type;
  }

  public ImmutableList<DistributionField> getFields() {
    return fields;
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
      return this.type == that.type && this.fields.equals(that.fields) ;
    }
    return false;
  }

  @Override
  public String toString() {
    return fields == null ? this.type.toString() : this.type.toString() + "(" + fields + ")";
  }

  
  public static class DistributionField {
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
