/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.Sets;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.stat.StatExpressions;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetToDrillTypeConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ParquetFilterEvaluator {

  public static boolean evalFilter(LogicalExpression expr, List<ColumnChunkMetaData> columnChunkMetaDatas) {
    FilterPredicate predicate = ParquetFilterBuilder.buildParquetFilterPredicate(expr);
    if (predicate != null) {
      return StatisticsFilter.canDrop(predicate, columnChunkMetaDatas);
    }
    return false;
  }

  public static boolean evalFilter(LogicalExpression expr, ParquetMetadata footer, int rowGroupIndex, OptionManager options) {
    // figure out the set of columns referenced in expression.
    final Collection<SchemaPath> schemaPathsInExpr = expr.accept(new FieldReferenceFinder(), null);
    final CaseInsensitiveMap<SchemaPath> columnInExprMap = CaseInsensitiveMap.newHashMap();
    for (final SchemaPath path : schemaPathsInExpr) {
      columnInExprMap.put(path.getRootSegment().getPath(), path);
    }

    // map from column name to ColumnDescriptor
    CaseInsensitiveMap<ColumnDescriptor> columnDescMap = CaseInsensitiveMap.newHashMap();
    for (final ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
      columnDescMap.put(column.getPath()[0], column);
    }

    // map from column name to SchemeElement
    final FileMetaData fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    final CaseInsensitiveMap<SchemaElement> schemaElementMap = CaseInsensitiveMap.newHashMap();
    for (final SchemaElement se : fileMetaData.getSchema()) {
      schemaElementMap.put(se.getName(), se);
    }

    // map from column name to ColumnChunkMetaData
    final CaseInsensitiveMap<ColumnChunkMetaData> columnStatMap = CaseInsensitiveMap.newHashMap();
    for (final ColumnChunkMetaData colMetaData: footer.getBlocks().get(rowGroupIndex).getColumns()) {
      columnStatMap.put(colMetaData.getPath().toDotString(), colMetaData);
    }

    // map from column name to column stat expression.
    CaseInsensitiveMap<StatExpressions.StatExpression> statExprMap = CaseInsensitiveMap.newHashMap();

    for (final String path : columnInExprMap.keySet()) {
      if (columnDescMap.containsKey(path) && schemaElementMap.containsKey(path) && columnDescMap.containsKey(path)) {
        ColumnDescriptor columnDesc =  columnDescMap.get(path);
        SchemaElement se = schemaElementMap.get(path);
        ColumnChunkMetaData metaData = columnStatMap.get(path);

        TypeProtos.MajorType mt = ParquetToDrillTypeConverter.toMajorType(columnDesc.getType(), se.getType_length(),
            getDataMode(columnDesc), se, options);

        StatExpressions.StatExpression statExpr = null;

        switch (mt.getMinorType()) {
          case INT:
            statExpr = new StatExpressions.IntStatExpression((IntStatistics) metaData.getStatistics(), metaData.getValueCount(), mt);
            break;
          case BIGINT:
            statExpr = new StatExpressions.LongStatExpression((LongStatistics) metaData.getStatistics(), metaData.getValueCount(), mt);
          default:
        }

        statExprMap.put(path, statExpr);
      }
    }


    return false;
  }

  private static TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxRepetitionLevel() > 0 ) {
      return TypeProtos.DataMode.REPEATED;
    } else if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  /**
   * Search through a LogicalExpression, finding all internal schema path references and returning them in a set.
   */
  private static class FieldReferenceFinder extends AbstractExprVisitor<Set<SchemaPath>, Void, RuntimeException> {
    @Override
    public Set<SchemaPath> visitSchemaPath(SchemaPath path, Void value) {
      Set<SchemaPath> set = Sets.newHashSet();
      set.add(path);
      return set;
    }

    @Override
    public Set<SchemaPath> visitUnknown(LogicalExpression e, Void value) {
      Set<SchemaPath> paths = Sets.newHashSet();
      for (LogicalExpression ex : e) {
        paths.addAll(ex.accept(this, null));
      }
      return paths;
    }
  }
}
