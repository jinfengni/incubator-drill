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

package org.apache.drill.exec.planner.logical;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
//import org.eigenbase.sql2rel.SqlToRelConverter.Side;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public abstract class DrillPushFilterPastJoinRule extends RelOptRule{
  
  public static final DrillPushFilterPastJoinRule FILTER_ON_JOIN =
      new DrillPushFilterPastJoinRule(
          operand(
              FilterRel.class,
              operand(JoinRel.class, any())),
          "DrillPushFilterPastJoinRule:filter") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          FilterRel filter = call.rel(0);
          JoinRel join = call.rel(1);
          perform(call, filter, join);
        }
      };

  public static final DrillPushFilterPastJoinRule JOIN =
      new DrillPushFilterPastJoinRule(
          operand(JoinRel.class, any()),
          "DrillPushFilterPastJoinRule:no-filter") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          JoinRel join = call.rel(0);
          perform(call, null, join);
        }
      };

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a DrillPushFilterPastJoinRule with an explicit root operand.
   */
  private DrillPushFilterPastJoinRule(
      RelOptRuleOperand operand,
      String id) {
    super(operand, "DrillPushFilterRule: " + id);
  }
  //~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call, FilterRel filter,
      JoinRelBase join) {
    final List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());

    if (filter == null) {
      // There is only the joinRel
      // make sure it does not match a cartesian product joinRel
      // (with "true" condition) otherwise this rule will be applied
      // again on the new cartesian product joinRel.
      boolean onlyTrueFilter = true;
      for (RexNode joinFilter : joinFilters) {
        if (!joinFilter.isAlwaysTrue()) {
          onlyTrueFilter = false;
          break;
        }
      }

      if (onlyTrueFilter) {
        return;
      }
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? RelOptUtil.conjunctions(filter.getCondition())
            : ImmutableList.<RexNode>of();

    List<RexNode> leftFilters = new ArrayList<RexNode>();
    List<RexNode> rightFilters = new ArrayList<RexNode>();

    // TODO - add logic to derive additional filters.  E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed = false;
    if (RelOptUtil.classifyFilters(
        join,
        aboveFilters,
        join.getJoinType() == JoinRelType.INNER,
        !join.getJoinType().generatesNullsOnLeft(),
        !join.getJoinType().generatesNullsOnRight(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.
    if (RelOptUtil.classifyFilters(
        join,
        joinFilters,
        false,
        !join.getJoinType().generatesNullsOnRight(),
        !join.getJoinType().generatesNullsOnLeft(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    if (!filterPushed) {
      return;
    }

    // create FilterRels on top of the children if any filters were
    // pushed to them
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelNode leftRel =
        createFilterOnRel(
            rexBuilder,
            join.getLeft(),
            leftFilters);
    RelNode rightRel =
        createFilterOnRel(
            rexBuilder,
            join.getRight(),
            rightFilters);

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    RexNode joinFilter;

    if (joinFilters.size() == 0) {
      // if nothing actually got pushed and there is nothing leftover,
      // then this rule is a no-op
      if ((leftFilters.size() == 0) && (rightFilters.size() == 0)) {
        return;
      }
      joinFilter = rexBuilder.makeLiteral(true);
    } else {
      joinFilter =
          RexUtil.composeConjunction(rexBuilder, joinFilters, true);
    }
    RelNode newJoinRel = join.copy(
      join.getCluster().traitSetOf(Convention.NONE),
      joinFilter,
      leftRel,
      rightRel,
      join.getJoinType());
    call.getPlanner().onCopy(join, newJoinRel);
    
    RelNode newJoinRel2 = pushJoinCondIntoProj((JoinRel) newJoinRel, leftRel, rightRel);

    // create a FilterRel on top of the join if needed
    RelNode newRel =
        createFilterOnRel(rexBuilder, newJoinRel2, aboveFilters);

    call.transformTo(newRel);
  }

  /**
   * If the filter list passed in is non-empty, creates a FilterRel on top of
   * the existing RelNode; otherwise, just returns the RelNode
   *
   * @param rexBuilder rex builder
   * @param rel        the RelNode that the filter will be put on top of
   * @param filters    list of filters
   * @return new RelNode or existing one if no filters
   */
  private RelNode createFilterOnRel(
      RexBuilder rexBuilder,
      RelNode rel,
      List<RexNode> filters) {
    RexNode andFilters =
        RexUtil.composeConjunction(rexBuilder, filters, false);
    if (andFilters.isAlwaysTrue()) {
      return rel;
    }
    return CalcRel.createFilter(rel, andFilters);
  }
  
  private RelNode pushJoinCondIntoProj(JoinRel joinRel, RelNode leftRel, RelNode rightRel) {
    final List<RexNode> extraLeftExprs = new ArrayList<RexNode>();
    final List<RexNode> extraRightExprs = new ArrayList<RexNode>();
    final int leftCount = leftRel.getRowType().getFieldCount();
    final int rightCount = rightRel.getRowType().getFieldCount();
    RexNode joinCond = joinRel.getCondition();
    JoinRelType joinType = joinRel.getJoinType();
    
    joinCond = pushDownJoinConditions(
        joinCond,
        leftCount,
        rightCount,
        extraLeftExprs,
        extraRightExprs);
    if (!extraLeftExprs.isEmpty()) {
      final List<RelDataTypeField> fields =
          leftRel.getRowType().getFieldList();
      leftRel = CalcRel.createProject(
          leftRel,
          new AbstractList<Pair<RexNode, String>>() {
            @Override
            public int size() {
              return leftCount + extraLeftExprs.size();
            }

            @Override
            public Pair<RexNode, String> get(int index) {
              if (index < leftCount) {
                RelDataTypeField field = fields.get(index);
                return Pair.<RexNode, String>of(
                    new RexInputRef(index, field.getType()),
                    field.getName());
              } else {
                return Pair.<RexNode, String>of(
                    extraLeftExprs.get(index - leftCount), null);
              }
            }
          },
          true);
    }
    if (!extraRightExprs.isEmpty()) {
      final List<RelDataTypeField> fields =
          rightRel.getRowType().getFieldList();
      final int newLeftCount = leftCount + extraLeftExprs.size();
      rightRel = CalcRel.createProject(
          rightRel,
          new AbstractList<Pair<RexNode, String>>() {
            @Override
            public int size() {
              return rightCount + extraRightExprs.size();
            }

            @Override
            public Pair<RexNode, String> get(int index) {
              if (index < rightCount) {
                RelDataTypeField field = fields.get(index);
                return Pair.<RexNode, String>of(
                    new RexInputRef(index, field.getType()),
                    field.getName());
              } else {
                return Pair.of(
                    RexUtil.shift(
                        extraRightExprs.get(index - rightCount),
                        -newLeftCount),
                    null);
              }
            }
          },
          true);
    }
    
    RelNode newJoin = new JoinRel(
        joinRel.getCluster(),
        leftRel,
        rightRel,
        joinCond,
        joinType,
        ImmutableSet.<String>of());
        /*createJoin(
        leftRel,
        rightRel,
        joinCond,
        joinType,
        ImmutableSet.<String>of()); */
    if (!extraLeftExprs.isEmpty() || !extraRightExprs.isEmpty()) {
      Mappings.TargetMapping mapping =
          Mappings.createShiftMapping(
              leftCount + extraLeftExprs.size()
                  + rightCount + extraRightExprs.size(),
              0, 0, leftCount,
              leftCount, leftCount + extraLeftExprs.size(), rightCount);
      return RelOptUtil.project(newJoin, mapping);
    }
    return newJoin;

  }
  
  
  /**
   * Pushes down parts of a join condition. For example, given
   * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
   * "emp" that computes the expression
   * "emp.deptno + 1". The resulting join condition is a simple combination
   * of AND, equals, and input fields.
   */
  private RexNode pushDownJoinConditions(
      RexNode node,
      int leftCount,
      int rightCount,
      List<RexNode> extraLeftExprs,
      List<RexNode> extraRightExprs) {
    switch (node.getKind()) {
    case AND:
    case OR:
    case EQUALS:
      RexCall call = (RexCall) node;
      List<RexNode> list = new ArrayList<RexNode>();
      List<RexNode> operands = Lists.newArrayList(call.getOperands());
      for (int i = 0; i < operands.size(); i++) {
        RexNode operand = operands.get(i);
        final int left2 = leftCount + extraLeftExprs.size();
        final int right2 = rightCount + extraRightExprs.size();
        final RexNode e =
            pushDownJoinConditions(
                operand,
                leftCount,
                rightCount,
                extraLeftExprs,
                extraRightExprs);
        final List<RexNode> remainingOperands = Util.skip(operands, i + 1);
        final int left3 = leftCount + extraLeftExprs.size();
        final int right3 = rightCount + extraRightExprs.size();
        fix(remainingOperands, left2, left3);
        fix(remainingOperands, left3 + right2, left3 + right3);
        fix(list, left2, left3);
        fix(list, left3 + right2, left3 + right3);
        list.add(e);
      }
      if (!list.equals(call.getOperands())) {
        return call.clone(call.getType(), list);
      }
      return call;
    case INPUT_REF:
    case LITERAL:
      return node;
    default:
      BitSet bits = RelOptUtil.InputFinder.bits(node);
      final int mid = leftCount + extraLeftExprs.size();
      switch (Side.of(bits, mid)) {
      case LEFT:
        extraLeftExprs.add(node);
        return new RexInputRef(mid, node.getType());
      case RIGHT:
        final int index2 = mid + rightCount + extraRightExprs.size();
        extraRightExprs.add(node);
        return new RexInputRef(index2, node.getType());
      case BOTH:
      case EMPTY:
      default:
        return node;
      }
    }
  }

  private void fix(List<RexNode> operands, int before, int after) {
    if (before == after) {
      return;
    }
    for (int i = 0; i < operands.size(); i++) {
      RexNode node = operands.get(i);
      operands.set(i, RexUtil.shift(node, before, after - before));
    }
  }
  
  /**
   * Categorizes whether a bit set contains bits left and right of a
   * line.
   */
  private enum Side {
    LEFT, RIGHT, BOTH, EMPTY;

    static Side of(BitSet bitSet, int middle) {
      final int firstBit = bitSet.nextSetBit(0);
      if (firstBit < 0) {
        return EMPTY;
      }
      if (firstBit >= middle) {
        return RIGHT;
      }
      if (bitSet.nextSetBit(middle) < 0) {
        return LEFT;
      }
      return BOTH;
    }
  }

}

// End PushFilterPastJoinRule.java


