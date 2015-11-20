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
package org.apache.drill.exec.store.hive.schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.hive.DrillHiveMetaStoreClient;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveStoragePlugin;
import org.apache.drill.exec.store.hive.HiveStoragePluginConfig;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class HiveSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveSchemaFactory.class);

  // MetaStoreClient created using process user credentials
  private final DrillHiveMetaStoreClient processUserMetastoreClient;
  private final HiveStoragePlugin plugin;
  private final Map<String, String> hiveConfigOverride;
  private final String schemaName;
  private final HiveConf hiveConf;
  private final boolean isDrillImpersonationEnabled;
  private final boolean isHS2DoAsSet;

  public HiveSchemaFactory(HiveStoragePlugin plugin, String name, Map<String, String> hiveConfigOverride) throws ExecutionSetupException {
    this.schemaName = name;
    this.plugin = plugin;

    this.hiveConfigOverride = hiveConfigOverride;
    hiveConf = new HiveConf();
    if (hiveConfigOverride != null) {
      for (Map.Entry<String, String> entry : hiveConfigOverride.entrySet()) {
        final String property = entry.getKey();
        final String value = entry.getValue();
        hiveConf.set(property, value);
        logger.trace("HiveConfig Override {}={}", property, value);
      }
    }

    isHS2DoAsSet = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    isDrillImpersonationEnabled = plugin.getContext().getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED);

    try {
      processUserMetastoreClient =
          DrillHiveMetaStoreClient.createNonCloseableClientWithCaching(hiveConf, hiveConfigOverride);
    } catch (MetaException e) {
      throw new ExecutionSetupException("Failure setting up Hive metastore client.", e);
    }
  }

  /**
   * Does Drill needs to impersonate as user connected to Drill when reading data from Hive warehouse location?
   * @return True when both Drill impersonation and Hive impersonation are enabled.
   */
  private boolean needToImpersonateReadingData() {
    return isDrillImpersonationEnabled && isHS2DoAsSet;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    DrillHiveMetaStoreClient mClientForSchemaTree = processUserMetastoreClient;
    if (isDrillImpersonationEnabled) {
      try {
        mClientForSchemaTree = DrillHiveMetaStoreClient.createClientWithAuthz(processUserMetastoreClient, hiveConf,
            hiveConfigOverride, schemaConfig.getUserName(), schemaConfig.getIgnoreAuthErrors());
      } catch (final TException e) {
        throw new IOException("Failure setting up Hive metastore client.", e);
      }
    }
    HiveSchema schema = new HiveSchema(schemaConfig, mClientForSchemaTree, schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class HiveSchema extends AbstractSchema {

    private final SchemaConfig schemaConfig;
    private final DrillHiveMetaStoreClient mClient;
    private HiveDatabaseSchema defaultSchema;

    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<String, List<String>> tableNamesCache;
    private final LoadingCache<TableName, Optional<DrillTable>> tableCache;

    public HiveSchema(final SchemaConfig schemaConfig, final DrillHiveMetaStoreClient mClient, final String name) {
      super(ImmutableList.<String>of(), name);
      this.schemaConfig = schemaConfig;
      this.mClient = mClient;

      databaseNamesCache = CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.MINUTES)
          .build(new CacheLoader<String, List<String>>() {
            @Override
            public List<String> load(String key) throws Exception {
              if (!"databases".equals(key)) {
                throw new UnsupportedOperationException();
              }
              synchronized (HiveSchema.this) {
                return mClient.getDatabases();
              }
            }
          });

      tableNamesCache = CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.MINUTES)
          .build(new CacheLoader<String, List<String>>() {
            @Override
            public List<String> load(String dbName) throws Exception {
              synchronized (HiveSchema.this) {
                return mClient.getTableNames(dbName);
              }
            }
          });

      tableCache = CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.MINUTES)
          .build(new CacheLoader<TableName, Optional<DrillTable>>() {
            @Override
            public Optional<DrillTable> load(TableName key) throws Exception {
              DrillTable table = getDrillTableHelper(key.databaseName, key.tableName);
              return Optional.fromNullable(table);
            }
          });

      getSubSchema("default");
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      Stopwatch watch = new Stopwatch();
      watch.start();

      List<String> tables;
      try {
        List<String> dbs = databaseNamesCache.get("databases");
        if (!dbs.contains(name)) {
          logger.debug("Database '{}' doesn't exists in Hive storage '{}'", name, schemaName);
          return null;
        }
        HiveDatabaseSchema schema = getSubSchemaKnownExists(name);
        if (name.equals("default")) {
          this.defaultSchema = schema;
        }
        logger.debug("Took {} ms to getSubSchema for '{}'  in Hive storage '{}'",  watch.elapsed(TimeUnit.MILLISECONDS), name, schemaName);
        return schema;
      } catch (final Exception e) {
        logger.warn("Failure while attempting to access HiveDatabase '{}'.", name, e.getCause());
        return null;
      }
    }

    /** Help method to get subschema when we know it exists (already checks the existence) */
    private HiveDatabaseSchema getSubSchemaKnownExists(String name) {
      Stopwatch watch = new Stopwatch();
      watch.start();

      HiveDatabaseSchema schema = new HiveDatabaseSchema(this, name);
      logger.debug("Took {} ms to getSubSchema for '{}'  in Hive storage '{}'",  watch.elapsed(TimeUnit.MILLISECONDS), name, schemaName);
      return schema;
    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String s : getSubSchemaNames()) {
        plusOfThis.add(s, getSubSchemaKnownExists(s));
      }
    }

    @Override
    public boolean showInInformationSchema() {
      return false;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      try {
        List<String> dbs = databaseNamesCache.get("databases");
        return Sets.newHashSet(dbs);
      } catch (final Exception e) {
        logger.warn("Failure while getting Hive database list.", e);
      }
      return super.getSubSchemaNames();
    }

    @Override
    public org.apache.calcite.schema.Table getTable(String name) {
      if (defaultSchema == null) {
        return super.getTable(name);
      }
      return defaultSchema.getTable(name);
    }

    @Override
    public Set<String> getTableNames() {
      if (defaultSchema == null) {
        return super.getTableNames();
      }
      return defaultSchema.getTableNames();
    }

    DrillTable getDrillTable(String dbName, String tableName) {
      try {
        Optional<DrillTable> table = tableCache.get(TableName.table(dbName, tableName));
        if (table.isPresent()) {
          return table.get();
        }
      } catch (final Exception e) {
        Throwable throwable = e.getCause();
        Throwables.propagateIfInstanceOf(throwable, UserException.class);
        logger.warn("Failure while getting Hive table {}:{}. Exception:{}. ", dbName, tableName, e);
      }
      return null;
    }

    DrillTable getDrillTableHelper(String dbName, String t) {
      HiveReadEntry entry = getSelectionBaseOnName(dbName, t);
      if (entry == null) {
        return null;
      }

      final String userToImpersonate = needToImpersonateReadingData() ? schemaConfig.getUserName() :
          ImpersonationUtil.getProcessUserName();

      if (entry.getJdbcTableType() == TableType.VIEW) {
        return new DrillHiveViewTable(schemaName, plugin, userToImpersonate, entry);
      } else {
        return new DrillHiveTable(schemaName, plugin, userToImpersonate, entry);
      }
    }

    HiveReadEntry getSelectionBaseOnName(String dbName, String t) {
      if (dbName == null) {
        dbName = "default";
      }
      try{
        return mClient.getHiveReadEntry(dbName, t);
      }catch(final TException e) {
        logger.warn("Exception occurred while trying to read table. {}.{}. Exception:{}", dbName, t, e);
        return null;
      }
    }

    @Override
    public AbstractSchema getDefaultSchema() {
      return defaultSchema;
    }

    @Override
    public String getTypeName() {
      return HiveStoragePluginConfig.NAME;
    }

    @Override
    public void close() throws Exception {
      if (mClient != null) {
        mClient.close();
      }
    }

    public List<String> getTableNamesFromMetaStore(String dbName) {
      try {
        return tableNamesCache.get(dbName);
      } catch (final ExecutionException e) {
        logger.warn("Failure while attempting to access HiveDatabase '{}'.", dbName, e.getCause());
        return null;
      }
    }
  }

  public static class TableName {
    private final String databaseName;
    private final String tableName;

    private TableName(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    public static TableName table(String databaseName, String tableName) {
      return new TableName(databaseName, tableName);
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public String toString() {
      return String.format("databaseName:%s, tableName:%s", databaseName, tableName).toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TableName other = (TableName) o;
      return Objects.equals(databaseName, other.databaseName) &&
          Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(databaseName, tableName);
    }
  }

}
