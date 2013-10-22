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
package org.apache.drill.exec.cache;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;

import com.google.common.collect.Maps;

public class LocalCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalCache.class);

  private volatile Map<FragmentHandle, PlanFragment> handles;
  private volatile ConcurrentMap<Class, org.apache.drill.exec.cache.Map> maps;
  private volatile ConcurrentMap<Class, MultiMap> multiMaps;
  private volatile ConcurrentMap<String, Counter> counters;
  
  @Override
  public void close() throws IOException {
    handles = null;
  }

  @Override
  public void run() throws DrillbitStartupException {
    handles = Maps.newConcurrentMap();
    maps = Maps.newConcurrentMap();
    multiMaps = Maps.newConcurrentMap();
    counters = Maps.newConcurrentMap();
  }

  @Override
  public PlanFragment getFragment(FragmentHandle handle) {
    logger.debug("looking for fragment with handle: {}", handle);
    return handles.get(handle);
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
    logger.debug("Storing fragment: {}", fragment);
    handles.put(fragment.getHandle(), fragment);
  }
  
  @Override
  public <V extends DrillSerializable> MultiMap<V> getMultiMap(Class<V> clazz) {
    MultiMap mmap = multiMaps.get(clazz);
    if (mmap == null) {
      multiMaps.putIfAbsent(clazz, new LocalMultiMapImpl<V>());
    } else {
      return mmap;
    }
    return multiMaps.get(clazz);
  }

  @Override
  public <V extends DrillSerializable> org.apache.drill.exec.cache.Map<V> getMap(Class<V> clazz) {
    org.apache.drill.exec.cache.Map m = maps.get(clazz);
    if (m == null) {
      maps.putIfAbsent(clazz, new LocalMapImpl());
    } else {
      return m;
    }
    return maps.get(clazz);
  }

  @Override
  public Counter getCounter(String name) {
    Counter c = counters.get(name);
    if (c == null) {
      counters.putIfAbsent(name, new LocalCounterImpl());
    } else {
      return c;
    }
    return counters.get(name);
  }

  public static class LocalMultiMapImpl<V> implements MultiMap<V> {
    private ArrayListMultimap<String, DrillSerializable> mmap;

    public LocalMultiMapImpl() {
      mmap = ArrayListMultimap.create();
    }

    @Override
    public Collection get(String key) {
      return mmap.get(key);
    }

    @Override
    public void put(String key, DrillSerializable value) {
      mmap.put(key, value);
    }
  }

  public static class LocalMapImpl<V> implements org.apache.drill.exec.cache.Map<V> {
    private ConcurrentMap<String, DrillSerializable> m;

    public LocalMapImpl() {
      m = Maps.newConcurrentMap();
    }

    @Override
    public DrillSerializable get(String key) {
      return m.get(key);
    }

    @Override
    public void put(String key, DrillSerializable value) {
      m.put(key, value);
    }

    @Override
    public void putIfAbsent(String key, DrillSerializable value) {
      m.putIfAbsent(key, value);
    }

    @Override
    public void putIfAbsent(String key, DrillSerializable value, long ttl, TimeUnit timeUnit) {
      m.putIfAbsent(key, value);
      logger.warn("Expiration not implemented in local map cache");
    }
  }

  public static class LocalCounterImpl implements Counter {
    private AtomicLong al = new AtomicLong();

    @Override
    public long get() {
      return al.get();
    }

    @Override
    public long incrementAndGet() {
      return al.incrementAndGet();
    }

    @Override
    public long decrementAndGet() {
      return al.decrementAndGet();
    }
  }
}
