package org.apache.drill.exec.planner.physical;

import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitDef;

/**
 * 
 * Drill operator's multiple mode:
 *
 */
public class DrillMuxMode implements RelTrait {
  public static enum MuxMode {NO_MUX, SIMPLEX, MULTIPLEX};
  
  public static DrillMuxMode DEFAULT = new DrillMuxMode(MuxMode.NO_MUX);
  
  public static DrillMuxMode SIMPLEX = new DrillMuxMode(MuxMode.SIMPLEX);
  public static DrillMuxMode MULTIPLEX = new DrillMuxMode(MuxMode.MULTIPLEX);
  
  private MuxMode mode;
  
  public DrillMuxMode(MuxMode mode) {
    this.mode = mode;
  }
  
  public boolean subsumes(RelTrait trait) {
    if(trait == DEFAULT) return true;
    return this == trait;
  }

  public RelTraitDef<DrillMuxMode> getTraitDef() {
    return DrillMuxModeDef.INSTANCE;
  }
  
  public int hashCode() {
    return mode.hashCode();
  }
  
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DrillMuxMode) {
      DrillMuxMode that = (DrillMuxMode) obj;
      return this.mode ==that.mode;
    }
    return false;
  }
  
  
  public MuxMode getMode() {
    return mode;
  }

  @Override
  public String toString() {
    return this.mode.toString();
  }
  
}
