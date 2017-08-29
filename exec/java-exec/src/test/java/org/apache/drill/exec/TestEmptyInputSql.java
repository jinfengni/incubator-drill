/*
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

package org.apache.drill.exec;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

import java.util.List;

public class TestEmptyInputSql extends BaseTestQuery {

  public final String SINGLE_EMPTY_JSON = "/scan/emptyInput/emptyJson/empty.json";
  public final String SINGLE_EMPTY_CSVH = "/scan/emptyInput/emptyCsvH/empty.csvh";
  public final String SINGLE_EMPTY_CSV = "/scan/emptyInput/emptyCsv/empty.csv";

  /**
   * Test with query against an empty file. Select clause has regular column reference, and an expression.
   *
   * regular column "key" is assigned with nullable-int
   * expression "key + 100" is materialized with nullable-int as output type.
   */
  @Test
  public void testQueryEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_JSON).toURI().toString();
    final String query = String.format("select key, key + 100 as key2 from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.INT)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key2"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test with query against an empty file. Select clause has one or more *
   * star column is expanded into an empty list.
   * @throws Exception
   */
  @Test
  public void testQueryStarColEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_JSON).toURI().toString();
    final String query1 = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    final String query2 = String.format("select *, * from dfs_test.`%s` ", rootEmpty);

    testBuilder()
        .sqlQuery(query2)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test with query against an empty file. Select clause has one or more qualified *
   * star column is expanded into an empty list.
   * @throws Exception
   */
  @Test
  public void testQueryQualifiedStarColEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_JSON).toURI().toString();
    final String query1 = String.format("select foo.* from dfs_test.`%s` as foo", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    final String query2 = String.format("select foo.*, foo.* from dfs_test.`%s` as foo", rootEmpty);

    testBuilder()
        .sqlQuery(query2)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

  }

  @Test
  public void testQueryMapArrayEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_JSON).toURI().toString();
    final String query = String.format("select foo.a.b as col1, foo.columns[2] as col2, foo.bar.columns[3] as col3 from dfs_test.`%s` as foo", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.INT)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col1"), majorType));

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col2"), majorType));

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col3"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test with query against an empty file. Select clause has three expressions.
   * 1.0 + 100.0 as constant expression, is resolved to required FLOAT8
   * cast(100 as varchar(100) is resolved to required varchar(100)
   * cast(columns as varchar(100)) is resolved to nullable varchar(100).
   */
  @Test
  public void testQueryConstExprEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_JSON).toURI().toString();
    final String query = String.format("select 1.0 + 100.0 as key, "
        + " cast(100 as varchar(100)) as name, "
        + " cast(columns as varchar(100)) as name2 "
        + " from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.FLOAT8)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setPrecision(100)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("name"), majorType));

    majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setPrecision(100)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("name2"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testQueryEmptyCsvH() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_CSVH).toURI().toString();
    final String query1 = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testQueryEmptyCsv() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(SINGLE_EMPTY_CSV).toURI().toString();
    final String query1 = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setMode(TypeProtos.DataMode.REPEATED)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("columns"), majorType));

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

}
