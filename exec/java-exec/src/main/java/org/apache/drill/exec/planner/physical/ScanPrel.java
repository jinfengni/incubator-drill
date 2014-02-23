package org.apache.drill.exec.planner.physical;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrel extends TableAccessRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);

  private GroupScan scan;
  
  public ScanPrel(GroupScan scan, RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl) {
    super(cluster, traits, tbl);
    
    assert getConvention() == CONVENTION;
    DrillTable table = tbl.unwrap(DrillTable.class);
    Object selection = table.getSelection();
    StoragePlugin plugin = table.getPlugin();
    plugin.getPhysicalScan(selection)
    Scan.Builder builder = Scan.builder();
    builder.storageEngine(drillTable.getStorageEngineName());
    builder.selection(new JSONOptions(drillTable.getSelection()));
    //builder.outputReference(new FieldReference("_MAP"));
    implementor.registerSource(drillTable);
    return builder.build();

    this.drillTable = 
  }
  
  
}
