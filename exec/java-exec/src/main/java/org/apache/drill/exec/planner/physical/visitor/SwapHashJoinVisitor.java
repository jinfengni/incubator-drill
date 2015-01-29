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

package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.JoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;

import java.util.List;

public class SwapHashJoinVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  private static SwapHashJoinVisitor INSTANCE = new SwapHashJoinVisitor();

  public static Prel swapHashJoin(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  private SwapHashJoinVisitor() {

  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {
    JoinPrel newJoin = (JoinPrel) visitPrel(prel, value);

    if (prel instanceof HashJoinPrel) {
      //Mark left/right is swapped, when INNER hash join's left row count < right row count.
      if (newJoin.getLeft().getRows() < newJoin.getRight().getRows() &&
          newJoin.getJoinType() == JoinRelType.INNER) {
        ( (HashJoinPrel) newJoin).setSwapped(true);
      }
    }

    return newJoin;
  }

}
