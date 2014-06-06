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

package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;

public class NullableBigIntSingularReaderImpl extends AbstractFieldReader{
  private NullableBigIntHolder holder;

  private Long value; 
  
  public NullableBigIntSingularReaderImpl(NullableBigIntHolder holder) {
    this.holder = holder;
  }
  
  @Override
  public int size() {
    throw new UnsupportedOperationException("You can't call size on a singular value reader.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");
  }

  @Override
  public MajorType getType() {
    return this.holder.getType();
  }

  @Override
  public void setPosition(int index) {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");
  }

  @Override
  public boolean isSet() {
    return this.holder.isSet();
  }
  
  @Override
  public Long readLong(){
    if (value == null) {
      value = new Long(this.holder.value);
    }
    return value;
  }
  
}