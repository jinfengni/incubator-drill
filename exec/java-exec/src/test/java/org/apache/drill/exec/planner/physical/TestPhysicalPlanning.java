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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY from dfs.`/Users/jni/regions1/`");   
    worker.runPhysicalPlan(plan, c);
    
  }

  @Test
  public void testSimpleQueryMultiFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY from dfs.`/Users/jni/regions2/`");   
    worker.runPhysicalPlan(plan, c);
    
  }

  @Test
  public void testAggSingleFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY from dfs.`/Users/jni/regions1/` group by R_REGIONKEY");  
    worker.runPhysicalPlan(plan, c);
    
  }

  @Test
  public void testAggMultiFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY from dfs.`/Users/jni/regions2/` group by R_REGIONKEY");    
    worker.runPhysicalPlan(plan, c);

  }
 
  @Test
  public void testAggOrderByDiffGKeyMultiFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY, SUM(cast(R_REGIONKEY AS int)) As S from dfs.`/Users/jni/regions2/` group by R_REGIONKEY ORDER BY S");    
    worker.runPhysicalPlan(plan, c);

  }
 
  @Test
  public void testAggOrderBySameGKeyMultiFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY, SUM(cast(R_REGIONKEY AS int)) As S from dfs.`/Users/jni/regions2/` group by R_REGIONKEY ORDER BY R_REGIONKEY");   
    worker.runPhysicalPlan(plan, c);
  }
   
  @Test
  public void testJoinSingleFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select T1.R_REGIONKEY from dfs.`/Users/jni/regions1/` as T1 join dfs.`/Users/jni/nations1/` as T2 on T1.R_REGIONKEY = T2.N_REGIONKEY");   
    worker.runPhysicalPlan(plan, c);
  
  }

  @Test
  public void testJoinMultiFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select T1.R_REGIONKEY from dfs.`/Users/jni/regions2/` as T1 join dfs.`/Users/jni/nations2/` as T2 on T1.R_REGIONKEY = T2.N_REGIONKEY");   
    worker.runPhysicalPlan(plan, c);
  
  }
  
  @Test
  public void testSortSingleFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY from dfs.`/Users/jni/regions1/` order by R_REGIONKEY");   
    worker.runPhysicalPlan(plan, c);
  }

  @Test
  public void testSortMultiFile(final DrillbitContext bitContext) throws Exception{
    
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
    PhysicalPlan plan = worker.getPhysicalPlan("select R_REGIONKEY from dfs.`/Users/jni/regions2/` order by R_REGIONKEY");   
    worker.runPhysicalPlan(plan, c);
  }
  
  @AfterClass
  public static void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }

}
