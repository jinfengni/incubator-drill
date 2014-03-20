package org.apache.drill.exec.planner.physical;

import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class TestPhysicalPlanning {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPhysicalPlanning.class);

  @Test
  @Ignore
  public void testSimpleQuerySingleFile(final DrillbitContext bitContext) throws Exception{
    
    final DrillConfig c = DrillConfig.create();
    new NonStrictExpectations() {
      {
        bitContext.getMetrics();
        result = new MetricRegistry();
        bitContext.getAllocator();
        result = new TopLevelAllocator();
        bitContext.getConfig();
        result = c;
      }
    };
    
    FunctionRegistry reg = new FunctionRegistry(c);
    StoragePluginRegistry registry = new StoragePluginRegistry(bitContext);
    DrillSqlWorker worker = new DrillSqlWorker(registry.getSchemaFactory(), reg);
    //worker.getPhysicalPlan("select * from cp.`employee.json`");
    
  }
  
 public void testJoin(final DrillbitContext bitContext) throws Exception{
    
    final DrillConfig c = DrillConfig.create();
    new NonStrictExpectations() {
      {
        bitContext.getMetrics();
        result = new MetricRegistry();
        bitContext.getAllocator();
        result = new TopLevelAllocator();
        bitContext.getConfig();
        result = c;
      }
    };
    
    FunctionRegistry reg = new FunctionRegistry(c);
    StoragePluginRegistry registry = new StoragePluginRegistry(bitContext);
    DrillSqlWorker worker = new DrillSqlWorker(registry.getSchemaFactory(), reg);
    //worker.getPhysicalPlan("select T1.R_REGIONKEY from dfs.`/Users/jni/regions1/` as T1 join dfs.`/Users/jni/nations1/` as T2 on T1.R_REGIONKEY = T2.N_REGIONKEY;", );
    
  }
  
  @AfterClass
  public static void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }

}
