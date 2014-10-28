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

package org.apache.drill.exec.store.sys.local;

import com.google.common.collect.Maps;
import org.apache.drill.exec.store.sys.EStore;
import org.apache.drill.exec.store.sys.EStoreProvider;
import org.apache.drill.exec.store.sys.PStoreConfig;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class LocalEStoreProvider implements EStoreProvider{
  private ConcurrentMap<PStoreConfig<?>, EStore<?>> estores = Maps.newConcurrentMap();

  @Override
  public <V> EStore<V> getEStore(PStoreConfig<V> storeConfig) throws IOException {
    if (! (estores.containsKey(storeConfig)) ) {
      EStore<V> p = new MapEStore<V>();
      EStore<?> p2 = estores.putIfAbsent(storeConfig, p);
      if(p2 != null) {
        return (EStore<V>) p2;
      }
      return p;
    } else {
      return (EStore<V>) estores.get(storeConfig);
    }
  }
}
