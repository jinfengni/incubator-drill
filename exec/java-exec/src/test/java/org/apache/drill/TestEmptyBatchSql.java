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

package org.apache.drill;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

import java.util.List;

public class TestEmptyBatchSql extends  BaseTestQuery {

  @Test
  public void testQueryEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();
    final String query = String.format("select key from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.INT)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();

    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testQueryStarColEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();
    final String query = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testQueryConstExprEmptyJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();
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

}
