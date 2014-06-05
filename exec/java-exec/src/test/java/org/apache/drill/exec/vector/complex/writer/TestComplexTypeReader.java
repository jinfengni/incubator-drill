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

package org.apache.drill.exec.vector.complex.writer;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestExampleQueries;
import org.junit.Ignore;
import org.junit.Test;

public class TestComplexTypeReader extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test
  // Repeated map (map) -> json. Works.
  public void testX() throws Exception{
    test("select convert_to(z[0], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //map -> json. works.
  public void testX2() throws Exception{
    test("select convert_to(x, 'JSON') from cp.`jsoninput/input2.json`;");
  }


  @Test
  @Ignore  //Map (mapfield) -> json.  Missing function Impl.
  public void testX3() throws Exception{
    test("select convert_to(x['y'], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  @Ignore //RepeatedMap (Map  (mapfield)) -> Json. Not Working! Missing Fucntion Implementation.
  public void testX4() throws Exception{
    test("select convert_to(z[0]['orange'], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  // repeated map -> json. Works fine.
  public void testX6() throws Exception{
    test("select convert_to(z, 'JSON')  from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt) -> json
  public void testX7() throws Exception{
    test("select convert_to(rl[1], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  @Ignore
  //repeated list (Repeated BigInt) -> json
  public void testX8() throws Exception{
    test("select convert_to(rl[1][2], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list -> json
  public void testX9() throws Exception{
    test("select convert_to(rl, 'JSON') from cp.`jsoninput/input2.json`;");
  }


  @Test
  public void testY() throws Exception{
    test("select z[0] from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY2() throws Exception{
    test("select x from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY3() throws Exception{
    test("select x['y'] from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY6() throws Exception{
    test("select z  from cp.`jsoninput/input2.json`;");
  }

  @Test
  @Ignore //repeated list (Repeated BigInt) -> json
  public void testZ() throws Exception{
    test("select rl[1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ) --> Json
  public void testZ1() throws Exception{
    test("select rl[0][1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ) --> Json. The first index is out of boundary
  public void testZ2() throws Exception{
    test("select rl[1000][1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ) --> Json. The second index is out of boundary
  public void testZ3() throws Exception{
    test("select rl[0][1000] from cp.`jsoninput/input2.json`;");
  }
  
  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ) --> Json. The second index is out of boundary
  public void testU() throws Exception{
    test("select nestrl['x'][1][0] from dfs.`/Users/jni/work/query/ComplexReader/inputMy.json`;");
  }
  
}
