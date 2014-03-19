package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.common.logical.PlanProperties.PlanPropertiesBuilder;
import org.apache.drill.common.logical.PlanProperties.PlanType;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillParseContext;

import com.google.common.collect.Lists;


public class PhysicalPlanCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlanCreator.class);

  private List<PhysicalOperator> popList;
  private final DrillParseContext context;
  PhysicalPlan plan = null;
  
  public PhysicalPlanCreator(DrillParseContext context) {
    this.context = context;
    popList = Lists.newArrayList();
  }
  
  public DrillParseContext getContext() {
    return context;
  }
  
  public void addPhysicalOperator(PhysicalOperator op) {
    popList.add(op);  
  }
  
  public PhysicalPlan build(Prel rootPrel, boolean forceRebuild) {

    if (plan != null && !forceRebuild) {
      return plan;
    }
    
    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.APACHE_DRILL_PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(PhysicalPlanCreator.class.getName(), "");

    
    try { 
      // invoke getPhysicalOperator on the root Prel which will recursively invoke it 
      // on the descendants and we should have a well-formed physical operator tree
      PhysicalOperator rootPOP = rootPrel.getPhysicalOperator(this).getPhysicalOperator();
      if (rootPOP != null) {
        assert (popList.size() > 0); //getPhysicalOperator() is supposed to populate this list 
        plan = new PhysicalPlan(propsBuilder.build(), popList);
      }
      
    } catch (IOException e) {
      plan = null;
      throw new UnsupportedOperationException("Physical plan created failed with error : " + e.toString());
    }
    
    return plan;
  }

}
