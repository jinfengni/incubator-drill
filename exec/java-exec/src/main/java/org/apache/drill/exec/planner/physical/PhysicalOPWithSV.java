package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

public class PhysicalOPWithSV {
  private final PhysicalOperator pop;
  private final SelectionVectorMode svMode;
  
  public PhysicalOPWithSV(PhysicalOperator pop, SelectionVectorMode svMode) {
    this.pop = pop;
    this.svMode = svMode;
  }
  
  public PhysicalOperator getPhysicalOperator() {
    return this.pop;
  }
  
  public SelectionVectorMode getSVMode() {
    return this.svMode;
  }
}
