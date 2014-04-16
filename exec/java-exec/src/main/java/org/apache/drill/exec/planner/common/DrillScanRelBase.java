/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.common;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

import com.google.common.collect.Lists;


/**
 * Base class for logical and physical Scans implemented in Drill
 */
public abstract class DrillScanRelBase extends TableAccessRelBase implements DrillRelNode {
  protected final DrillTable drillTable;

  public DrillScanRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelOptTable table) {
    super(cluster, traits, table);
    this.drillTable = table.unwrap(DrillTable.class);
    assert drillTable != null;
  }

  public DrillScanRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelOptTable table, DrillTable drillTable) {
    super(cluster, traits, table);
    this.drillTable = drillTable;
    assert drillTable != null;
  }
  
  public List<SchemaPath> getColumns() { 
    boolean containStar = false;
    final List<String> fields = getRowType().getFieldNames();
    List<SchemaPath> columns = Lists.newArrayList();
    for (String field : fields) {
      columns.add(new SchemaPath(field));
  
      if (field.startsWith("*")) {
        containStar = true;  
        break;
      }
    }
    
    if (containStar) {
      columns = null;
    }
    return columns;
  }   

}
