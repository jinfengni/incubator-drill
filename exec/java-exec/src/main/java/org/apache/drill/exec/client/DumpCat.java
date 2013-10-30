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
package org.apache.drill.exec.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Lists;

public class DumpCat {

	private final static BufferAllocator allocator = BufferAllocator.getAllocator(DrillConfig.create());
			
	public static void main(String args[]) throws Exception {		
		DumpCat dumpCat = new DumpCat();

	  Options o = new Options();
    JCommander jc = null;
    try {
      jc = new JCommander(o, args);
      jc.setProgramName("./drill_dumpcat");
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      //String[] valid = {"-f", "file"};
      //new JCommander(o, valid).usage();
      jc.usage();
      System.exit(-1);
    }
    if (o.help) {
      jc.usage();
      System.exit(0);
    }
    
    if (o.batch < 0) {
    	dumpCat.doQuery(o.location); 	
    } else {
    	dumpCat.doBatch(o.location, o.batch, o.include_headers);
    }
	}
	
	public static class BatchNumValidator implements IParameterValidator {
		@Override
		public void validate(String name, String value) throws ParameterException {
			try {
				int batch = Integer.parseInt(value);
				if(batch < 0) {
		    	throw new ParameterException("Parameter " + name + " should be non-negative number.");
		    }
			} catch (NumberFormatException e) {
				throw new ParameterException("Parameter " + name + " should be non-negative number.");
			}
	    
	  }
	}
	
	static class Options {
		@Parameter(names = {"-f"}, description = "file containing dump", required=true)
	  public String location = null;

	  @Parameter(names = {"-batch"}, description = "id of batch to show", required=false, validateWith = BatchNumValidator.class)
	  public int batch = -1;
	    
	  @Parameter(names = {"-include-headers"}, description = "whether include header of batch", required=false)
	  public boolean include_headers = false;

	  @Parameter(names = {"-h", "-help", "--help"}, description = "show usage", help=true)
	  public boolean help = false;
	 }

	/**
	 * Contains : # of rows, # of selected rows, data size (byte #).  
	 */
	private class BatchMetaInfo {
		long rows = 0;
		long selectedRows = 0;
		long dataSize = 0;
		
		public BatchMetaInfo () {			
		}
		
		public BatchMetaInfo (long rows, long selectedRows, long dataSize) {
			this.rows = rows;
			this.selectedRows = selectedRows;
			this.dataSize = dataSize;
		}
		
		public void add(BatchMetaInfo info2) {
			this.rows += info2.rows;
			this.selectedRows += info2.selectedRows;
			this.dataSize += info2.dataSize;			
		}
		
		public String toString() {
			return String.format("Records : %d / %d \n", this.selectedRows, this.rows) +    	
			       String.format("Average Record Size : %d \n", this.dataSize / this.rows) + 
			       String.format("Total Data Size : %d", this.dataSize);  	
		}
	}
	 
	/** 
	 * Querymode:
	 * $drill-dumpcat --file=local:///tmp/drilltrace/[queryid]_[tag]_[majorid]_[minor]_[operator]
	 *   Batches: 135
   *   Records: 53,214/53,214 // the first one is the selected records.  The second number is the total number of records.
   *   Selected Records: 53,214
   *   Average Record Size: 74 bytes
   *   Total Data Size: 12,345 bytes
   *   Number of Empty Batches: 1
   *   Schema changes: 1
   *   Schema change batch indices: 0 
	 * @throws Exception
	 */
	private void doQuery(String dumpFilename) throws Exception{
		File file = new File(dumpFilename);

    if (!file.exists())
    	throw new IOException(String.format("Trace file %s not created", dumpFilename));

    FileInputStream input = new FileInputStream(file.getAbsoluteFile());

    int  batchNum = 0;
  	int  emptyBatchNum = 0;
  	BatchSchema prevSchema = null;
  	
  	List<Integer> schemaChangeIdx = Lists.newArrayList();
  	
  	BatchMetaInfo aggBatchMetaInfo = new BatchMetaInfo();
  	
    while (input.available() > 0) {    	
    	VectorAccessibleSerializable vcSerializable = new VectorAccessibleSerializable(DumpCat.allocator);
    	vcSerializable.readFromStream(input);
   		VectorContainer vectorContainer = (VectorContainer) vcSerializable.get();
   		
   		aggBatchMetaInfo.add(getBatchMetaInfo(vcSerializable));
   		
   		if (vectorContainer.getRecordCount() == 0) {
   			emptyBatchNum ++;
   		}
   		
   		if (prevSchema != null && !vectorContainer.getSchema().equals(prevSchema))
   			schemaChangeIdx.add(batchNum);
   		
   		prevSchema = vectorContainer.getSchema();
   	  batchNum ++;
   	  
 		  vectorContainer.zeroVectors();
    }
	   
   	input.close();
   	
   	/* output the summary stat */   	
   	System.out.println(String.format("Total # of batches: %d", batchNum));
   	//rows, selectedRows, avg rec size, total data size. 
   	System.out.println(aggBatchMetaInfo.toString());    
   	System.out.println(String.format("Empty batch : %d", emptyBatchNum));
	  System.out.println(String.format("Schema changes : %d", schemaChangeIdx.size()));
	  System.out.println(String.format("Schema change batch index : %s", schemaChangeIdx.toString()));	  	  
	}
	
	/**
	 * Batch mode: 
	 * $drill-dumpcat --file=local:///tmp/drilltrace/[queryid]_[tag]_[majorid]_[minor]_[operator] --batch=123 --include-headers=true
	 * Records: 1/1
   * Average Record Size: 8 bytes
   * Total Data Size: 8 bytes
   * Schema Information
   * name: col1, minor_type: int4, data_mode: nullable
   * name: col2, minor_type: int4, data_mode: non-nullable
	 * @param targetBatchNum
	 * @throws Exception
	 */
	private void doBatch(String dumpFilename, int targetBatchNum, boolean showHeader) throws Exception {
		File file = new File(dumpFilename);

		if (!file.exists())
    	throw new IOException(String.format("Trace file %s not created", dumpFilename));

    FileInputStream input = new FileInputStream(file.getAbsoluteFile());
   
    int batchNum = -1;
    
    VectorAccessibleSerializable vcSerializable = null;
    
    while (input.available() > 0 && batchNum ++ < targetBatchNum) {
    	vcSerializable = new VectorAccessibleSerializable(DumpCat.allocator);
    	vcSerializable.readFromStream(input);    	
    	
    	if (batchNum != targetBatchNum) {
    		VectorContainer vectorContainer = (VectorContainer) vcSerializable.get();
    		vectorContainer.zeroVectors();
    	}
    }
    
    input.close();
	  
    //
    if (batchNum < targetBatchNum) {
    	System.out.println(String.format("Wrong input of batch # ! Total # of batch in the file is %d. Please input a number 0..%d as batch #", batchNum+1, batchNum));
    	System.exit(-1);
    }
   	
    if (vcSerializable != null) {
    	showSingleBatch(vcSerializable, showHeader);
  		VectorContainer vectorContainer = (VectorContainer) vcSerializable.get();
  		vectorContainer.zeroVectors();
    }
	}
	
	
	private void showSingleBatch (VectorAccessibleSerializable vcSerializable, boolean showHeader) {
		VectorContainer vectorContainer = (VectorContainer)vcSerializable.get();   

		if (showHeader) {			
			System.out.println(getBatchMetaInfo(vcSerializable).toString());
			
			System.out.println("Schema Information"); 	
			for (VectorWrapper w : vectorContainer) {
				MaterializedField field = w.getValueVector().getField();
				System.out.println (String.format("name : %s, minor_type : %s, data_mode : %s", 
																					field.getName(), 
																					field.getType().getMinorType().toString(),
																					field.isNullable() ? "nullable":"non-nullable"
													));
			}  	
		}
		
		/* show the contents in the batch */		
   	List<String> columns = Lists.newArrayList();
    for (VectorWrapper vw : vectorContainer) {
      columns.add(vw.getValueVector().getField().getName());
    }
    
    int width = columns.size();
    int rows = vectorContainer.getRecordCount();
    
    for (int row = 0; row < rows; row++) {
      if (row%50 == 0) {
        System.out.println(StringUtils.repeat("-", width*17 + 1));
        for (String column : columns) {
          System.out.printf("| %-15s", width <= 15 ? column : column.substring(0, 14));
        }
        System.out.printf("|\n");
        System.out.println(StringUtils.repeat("-", width*17 + 1));
      }
      for (VectorWrapper vw : vectorContainer) {
        Object o = vw.getValueVector().getAccessor().getObject(row);
        if (o instanceof byte[]) {
          String value = new String((byte[]) o);
          System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0, 14));
        } else {
          String value = o.toString();
          System.out.printf("| %-15s",value.length() <= 15 ? value : value.substring(0,14));
        }
      }
      System.out.printf("|\n");
    }
	}
	
	private BatchMetaInfo getBatchMetaInfo(VectorAccessibleSerializable vcSerializable) {	
	 	VectorAccessible vectorContainer = vcSerializable.get();   
    	
  	int rows =0;
  	int selectedRows = 0;
  	int totalDataSize = 0;
  	
  	rows = vectorContainer.getRecordCount();
  	selectedRows = rows;
  	
  	if (vectorContainer.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE) {
  		selectedRows = vcSerializable.getSelectionVector2().getCount();
  	}
  	  	  	
  	for (VectorWrapper w : vectorContainer) {
   		totalDataSize += w.getValueVector().getBufferSize();
  	}
  	 	
		return new 	BatchMetaInfo(rows, selectedRows, totalDataSize);		
	}
		
}