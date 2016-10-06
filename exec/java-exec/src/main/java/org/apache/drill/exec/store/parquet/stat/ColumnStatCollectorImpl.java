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
package org.apache.drill.exec.store.parquet.stat;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetToDrillTypeConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.joda.time.DateTimeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ColumnStatCollectorImpl implements ColumnStatCollector {

  private final ParquetMetadata footer;
  private final int rowGroupIndex;
  private final OptionManager options;
  private final Map<String, String> implicitColValues;

  public ColumnStatCollectorImpl(ParquetMetadata footer, int rowGroupIndex, Map<String, String> implicitColValues, OptionManager options) {
    this.footer = footer;
    this.rowGroupIndex = rowGroupIndex;
    this.options = options;
    this.implicitColValues = implicitColValues;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> collectColStat(Set<SchemaPath> fields) {
    // map from column name to ColumnDescriptor
    Map<SchemaPath, ColumnDescriptor> columnDescMap = new HashMap<>();

    // map from column name to ColumnChunkMetaData
    final Map<SchemaPath, ColumnChunkMetaData> columnChkMetaMap = new HashMap<>();

    // map from column name to MajorType
    final Map<SchemaPath, TypeProtos.MajorType> columnTypeMap = new HashMap<>();

    // map from column name to SchemaElement
    final Map<SchemaPath, SchemaElement> schemaElementMap = new HashMap<>();

    // map from column name to column statistics.
    final Map<SchemaPath, ColumnStatistics> statMap = new HashMap<>();

    final org.apache.parquet.format.FileMetaData fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);

    for (final ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
      final SchemaPath schemaPath = SchemaPath.getCompoundPath(column.getPath());
      if (fields.contains(schemaPath)) {
        columnDescMap.put(schemaPath, column);
      }
    }

    for (final SchemaElement se : fileMetaData.getSchema()) {
      final SchemaPath schemaPath = SchemaPath.getSimplePath(se.getName());
      if (fields.contains(schemaPath)) {
        schemaElementMap.put(schemaPath, se);
      }
    }

//    final long rowCount = footer.getBlocks().get(rowGroupIndex).getRowCount();
    for (final ColumnChunkMetaData colMetaData: footer.getBlocks().get(rowGroupIndex).getColumns()) {

//      if (rowCount != colMetaData.getValueCount()) {
//        logger.warn("rowCount : {} for rowGroup {} is different from column {}'s valueCount : {}",
//            rowCount, rowGroupIndex, colMetaData.getPath().toDotString(), colMetaData.getValueCount());
//        return false;
//      }
      final SchemaPath schemaPath = SchemaPath.getCompoundPath(colMetaData.getPath().toArray());
      if (fields.contains(schemaPath)) {
        columnChkMetaMap.put(schemaPath, colMetaData);
      }
    }

    for (final SchemaPath path : fields) {
      if (columnDescMap.containsKey(path) && schemaElementMap.containsKey(path) && columnChkMetaMap.containsKey(path)) {
        ColumnDescriptor columnDesc =  columnDescMap.get(path);
        SchemaElement se = schemaElementMap.get(path);
        ColumnChunkMetaData metaData = columnChkMetaMap.get(path);

        TypeProtos.MajorType type = ParquetToDrillTypeConverter.toMajorType(columnDesc.getType(), se.getType_length(),
            getDataMode(columnDesc), se, options);

        columnTypeMap.put(path, type);

        if (metaData != null) {
          Statistics stat = convertStatIfNecessary(metaData.getStatistics(), type.getMinorType());
          statMap.put(path, new ColumnStatistics(stat, type));
        }
      } else {
        final String columnName = path.getRootSegment().getPath();
        if (implicitColValues.containsKey(columnName)) {
          TypeProtos.MajorType type = Types.required(TypeProtos.MinorType.VARCHAR);
          Statistics stat = new BinaryStatistics();
          stat.setNumNulls(0);
          byte[] val = implicitColValues.get(columnName).getBytes();
          stat.setMinMaxFromBytes(val, val);
          statMap.put(path, new ColumnStatistics(stat, type));
        }
      }
    }

    return statMap;
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

  private static Statistics convertStatIfNecessary(Statistics stat, TypeProtos.MinorType type) {
    if (type != TypeProtos.MinorType.DATE) {
      return stat;
    } else {
      IntStatistics dateStat = (IntStatistics) stat;
      LongStatistics dateMLS = new LongStatistics();
      if (!dateStat.isEmpty()) {
        dateMLS.setMinMax(convertToDrillDateValue(dateStat.getMin()), convertToDrillDateValue(dateStat.getMax()));
        dateMLS.setNumNulls(dateStat.getNumNulls());
      }

      return dateMLS;
    }
  }

  private static long convertToDrillDateValue(int dateValue) {
    long  dateInMillis = DateTimeUtils.fromJulianDay(dateValue - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5);
    //    // Specific for date column created by Drill CTAS prior fix for DRILL-4203.
    //    // Apply the same shift as in ParquetOutputRecordWriter.java for data value.
    //    final int intValue = (int) (DateTimeUtils.toJulianDayNumber(dateInMillis) + JULIAN_DAY_EPOC);
    //    return intValue;
    return dateInMillis;

  }

}
