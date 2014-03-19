package org.apache.drill.exec.planner.physical;

import java.io.IOException;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.DrillRelNode;
import org.eigenbase.relopt.Convention;

public interface Prel extends DrillRelNode {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Prel.class);
  
  final static Convention DRILL_PHYSICAL = new Convention.Impl("PHYSICAL", Prel.class);
  
  //public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException;
  
  public PhysicalOPWithSV getPhysicalOperator(PhysicalPlanCreator creator) throws IOException;
    
}
