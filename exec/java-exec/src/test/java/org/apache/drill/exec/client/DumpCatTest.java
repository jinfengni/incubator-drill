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

import java.io.FileInputStream;

import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

public class DumpCatTest {

	/* Test the drill_dumpcat tool under batch mode : output the header and context of one batch */
	
	@Test
  public void testBatchMode() throws Exception {
		
		DumpCat dumpCat = new DumpCat();
		
		FileInputStream input = new FileInputStream(FileUtils.getResourceAsFile("/trace/singlebatch.trace").getAbsoluteFile());

		dumpCat.doBatch(input, 0, true);
		
		input.close();
	}

	/* Test the drill_dumpcat tool under query mode : output the summary statistis of trace */
	@Test
  public void testQueryMode() throws Exception {
		
		DumpCat dumpCat = new DumpCat();
		
		FileInputStream input = new FileInputStream(FileUtils.getResourceAsFile("/trace/singlebatch.trace").getAbsoluteFile());

		dumpCat.doQuery(input);
		
		input.close();
	}	
}
