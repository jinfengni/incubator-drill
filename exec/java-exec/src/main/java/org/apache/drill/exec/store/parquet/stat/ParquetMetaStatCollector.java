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
package org.apache.drill.exec.store.parquet.stat;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.store.parquet.Metadata;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetMetaStatCollector implements  ColumnStatCollector{
  private  final Metadata.ParquetTableMetadataBase parquetTableMetadata;
  private  final List<? extends Metadata.ColumnMetadata> columnMetadataList;
  private  final long rowCount;
  final Map<String, String> implicitColValues;

  public ParquetMetaStatCollector(Metadata.ParquetTableMetadataBase parquetTableMetadata, List<? extends Metadata.ColumnMetadata> columnMetadataList, long rowCount, Map<String, String> implicitColValues) {
    this.parquetTableMetadata = parquetTableMetadata;
    this.columnMetadataList = columnMetadataList;
    this.rowCount = rowCount;
    this.implicitColValues = implicitColValues;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> collectColStat(Set<SchemaPath> fields) {
    // map from column to ColumnMetadata
    final Map<SchemaPath, Metadata.ColumnMetadata> columnMetadataMap = new HashMap<>();

    // map from column to PrimitiveTypeName
    final Map<SchemaPath, PrimitiveType.PrimitiveTypeName> columnPrimitiveTypeMap = new HashMap<>();

    // map from column to OriginalType
    final Map<SchemaPath, OriginalType> columnOriginalTypeMap = new HashMap<>();

    // map from column name to column statistics.
    final Map<SchemaPath, ColumnStatistics> statMap = new HashMap<>();

    for (final Metadata.ColumnMetadata columnMetadata : columnMetadataList) {
      SchemaPath schemaPath = SchemaPath.getCompoundPath(columnMetadata.getName());
      if (! fields.contains(schemaPath)) {
        continue; // this filed is not needed.
      }

      final PrimitiveType.PrimitiveTypeName primitiveType;
      final OriginalType originalType;

      if (this.parquetTableMetadata.hasColumnMetadata()) {
        primitiveType = this.parquetTableMetadata.getPrimitiveType(columnMetadata.getName());
        originalType = this.parquetTableMetadata.getOriginalType(columnMetadata.getName());
      } else {
        primitiveType = columnMetadata.getPrimitiveType();
        originalType = columnMetadata.getOriginalType();
      }

      columnMetadataMap.put(schemaPath, columnMetadata);
      columnPrimitiveTypeMap.put(schemaPath, primitiveType);
      columnOriginalTypeMap.put(schemaPath, originalType);
    }

    for (final SchemaPath schemaPath : fields) {
      final Metadata.ColumnMetadata columnMetadata = columnMetadataMap.get(schemaPath);
      final PrimitiveType.PrimitiveTypeName primitiveType = columnPrimitiveTypeMap.get(schemaPath);
      final OriginalType originalType = columnOriginalTypeMap.get(schemaPath);

      if (columnMetadata != null) {
        final Object min = columnMetadata.getMinValue();
        final Object max = columnMetadata.getMaxValue();
        final Long numNull = columnMetadata.getNulls();
        statMap.put(schemaPath, getStat(min, max, numNull, primitiveType, originalType));
      } else {
        final String columnName = schemaPath.getRootSegment().getPath();
        if (implicitColValues.containsKey(columnName)) {
          TypeProtos.MajorType type = Types.required(TypeProtos.MinorType.VARCHAR);
          Statistics stat = new BinaryStatistics();
          stat.setNumNulls(0);
          byte[] val = implicitColValues.get(columnName).getBytes();
          stat.setMinMaxFromBytes(val, val);
          statMap.put(schemaPath, new ColumnStatistics(stat, type));
        }
      }
    }

    return statMap;
  }

  private ColumnStatistics getStat(Object min, Object max, Long numNull, PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    Statistics stat = Statistics.getStatsBasedOnType(primitiveType);

    TypeProtos.MajorType type = ParquetGroupScan.getType(primitiveType, originalType);

    if (numNull != null) {
      stat.setNumNulls(numNull.longValue());
    }

    if (min != null && max != null ) {
      switch (type.getMinorType()) {
      case INT:
        ((IntStatistics) stat).setMinMax(((Integer) min).intValue(), ((Integer) max).intValue());
        break;
      case BIGINT:
        ((LongStatistics) stat).setMinMax(((Long) min).longValue(), ((Long) max).longValue());
        break;
      case FLOAT4:
        ((FloatStatistics) stat).setMinMax(((Float) min).floatValue(), ((Float) max).floatValue());
        break;
      case FLOAT8:
        ((DoubleStatistics) stat).setMinMax(((Double) min).doubleValue(), ((Double) max).doubleValue());
        break;
      case DATE:
        ((IntStatistics) stat).setMinMax(((Integer) min).intValue(), ((Integer) max).intValue());
      }
    }

    final Statistics convertedStat = ParquetFooterStatCollector.convertStatIfNecessary(stat, type.getMinorType());
    return new ColumnStatistics(convertedStat, type);
  }
}
