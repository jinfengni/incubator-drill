package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.physical.base.GroupScan;

public interface DrillScanPrel extends Prel{
  
  public GroupScan getGroupScan();
  
}
