package org.apache.drill.exec.planner.physical;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

public interface Prel extends RelNode {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Prel.class);
  
  final static Convention DRILL_PHYSICAL = new Convention.Impl("DRILL_PHYSICAL", Prel.class);
}
