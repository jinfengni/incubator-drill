package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;
import org.apache.drill.common.expression.SchemaPath;

import com.google.hive12.common.collect.Lists;

public class PushProjectIntoScan  extends RelOptRule {
  public static final RelOptRule INSTANCE = new PushProjectIntoScan();

  private PushProjectIntoScan() {
    super(RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class)), "PushProjectIntoScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
      final ProjectPrel proj = (ProjectPrel) call.rel(0);
      final ScanPrel scan = (ScanPrel) call.rel(1);
    
      DrillTable drillTable = scan.getTable().unwrap(DrillTable.class);
      StoragePlugin plugin = drillTable.getPlugin();    
     
      List<Integer> columnsIds = getRefColumnIds(proj);
     
      try {
        GroupScan groupScan = plugin.getPhysicalScan(new JSONOptions(drillTable.getSelection()), getColumns(scan.getRowType(), columnsIds));
        RelDataType newScanRowType = createStructType(scan.getCluster().getTypeFactory(), getProjectedFields(scan.getRowType(),columnsIds));
        
        final DrillScanPrel newScan = new GroupScanPrel(scan.getCluster(), proj.getTraitSet(), groupScan, newScanRowType);
        
        final ProjectPrel newProj = new ProjectPrel(proj.getCluster(), proj.getTraitSet(), newScan, proj.getProjects(), proj.getRowType());
        
        call.transformTo(newProj);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
  }
  
  private  RelDataType createStructType(
      RelDataTypeFactory typeFactory,
      final List<RelDataTypeField> fields
      ) {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        typeFactory.builder();
    for (RelDataTypeField field : fields) {
      builder.add(field.getName(), field.getType());
    }
    return builder.build();
  }
  

  private List<Integer> getRefColumnIds(ProjectPrel proj) {   
    RefFieldsVisitor v = new RefFieldsVisitor();
    
    for (RexNode exp : proj.getProjects()) {
      v.apply(exp);
    }    
    //System.out.println(v.getReferencedFieldIndex());
    return new ArrayList<Integer>(v.getReferencedFieldIndex());
  }
  
  private List<RelDataTypeField> getProjectedFields(RelDataType rowType, List<Integer> columnIds) {
    List<RelDataTypeField> oldFields = rowType.getFieldList();
    List<RelDataTypeField> newFields = Lists.newArrayList();

    for (Integer id : columnIds) {
      newFields.add(oldFields.get(id));
    }
    
    return newFields;    
  }
  
  private List<SchemaPath> getColumns(RelDataType rowType, List<Integer> columnIds) {
    List<SchemaPath> columns = Lists.newArrayList();
    final List<String> fields = rowType.getFieldNames();
    for (Integer id : columnIds) {
      columns.add(SchemaPath.getSimplePath(fields.get(id)));
    }
    
    return columns;
  }
  
  /** Visitor that finds the set of inputs that are used. */
  public static class RefFieldsVisitor extends RexShuttle {
    public final SortedSet<Integer> inputPosReferenced =
        new TreeSet<Integer>();

    public RexNode visitInputRef(RexInputRef inputRef) {
      inputPosReferenced.add(inputRef.getIndex());
      return inputRef;
    }
    
    public Set<Integer> getReferencedFieldIndex() {
      return this.inputPosReferenced;
    }
  }
  
}