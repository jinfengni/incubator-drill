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

package org.apache.drill.exec.planner.sql.rewriter;

import com.google.common.collect.ImmutableList;
import net.hydromatic.optiq.prepare.OptiqSqlValidator;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.SqlBasicCall;

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.validate.SelectScope;
import org.eigenbase.util.Util;

import java.util.List;


public class StarExpander {
  OptiqSqlValidator validator;


  public StarExpander(OptiqSqlValidator validator) {
    this.validator = validator;
  }

  public SqlNode expandStar(SqlNode sqlNode) {
    SqlNode newNode = expandQueryRecursive(sqlNode);
    return newNode;
  }

  public SqlNode expandQueryRecursive(SqlNode root) {
    switch (root.getKind()) {
    case SELECT:
      return expandSelect((SqlSelect) root, null);
    default:
      return root;
      // throw Util.newInternal("StarExpander.expandQueryRecursive not supported for : " + root);
    }
  }

  public SqlNode expandSelect(SqlSelect select, SqlSelect parent) {
    return expandSelectImpl(select, parent);
  }

  protected SqlNode expandSelectImpl(SqlSelect select, SqlSelect parent) {
    SqlNodeList selectList = select.getSelectList();

    SqlIdentifier starColumn = findStarColumn(selectList);

    RelDataType rowType = ( (SelectScope) this.validator.getWhereScope(select)).getChildren().get(0).getRowType();

    if (rowType.getFieldNames().contains("*") && starColumn != null && parent != null) {

      List<SqlNode> list = selectList.getList();

      List<SqlIdentifier> sqlIdentifierList = SqlIdentifierFinder.findSqlIdentifier(parent);

      for (SqlIdentifier id : sqlIdentifierList) {
        final SqlNode exp = new SqlIdentifier(ImmutableList.of(Util.last(id.names)), starColumn.getParserPosition());
        list.add(exp);
      }

      selectList = new SqlNodeList(list, SqlParserPos.ZERO);

      select.setSelectList(selectList);
    }

    SqlNode newFrom = expandFrom(select.getFrom(), select);
    select.setFrom(newFrom);
    return select;
  }

  protected SqlNode expandFrom(SqlNode from, SqlSelect parent) {
    switch (from.getKind()) {
      case SELECT:
        return expandSelectImpl((SqlSelect)from, parent);
      case IDENTIFIER:
        return from;
      case AS:
        final SqlNode[] operands = ((SqlBasicCall) from).getOperands();
        SqlNode tbl = expandFrom(operands[0], parent);
        ((SqlBasicCall) from).setOperand(0, tbl);
        return from;

      default:
        return from;
    }
  }

  private SqlIdentifier findStarColumn(SqlNodeList selectList) {
    for (SqlNode selectItem : selectList) {
      if (selectItem instanceof SqlIdentifier) {
        SqlIdentifier identifier = (SqlIdentifier) selectItem;
        if (Util.last(identifier.names).equals("*")) {
          return identifier;
        }
      }
    }
    return null;
  }

}
